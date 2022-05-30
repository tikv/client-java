/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common;

import static org.tikv.common.util.ClientUtils.groupKeysByRegion;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.apiversion.RequestKeyV1RawCodec;
import org.tikv.common.apiversion.RequestKeyV1TxnCodec;
import org.tikv.common.apiversion.RequestKeyV2RawCodec;
import org.tikv.common.apiversion.RequestKeyV2TxnCodec;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.importer.ImporterStoreClient;
import org.tikv.common.importer.SwitchTiKVModeClient;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Pdpb;
import org.tikv.raw.RawKVClient;
import org.tikv.raw.SmartRawKVClient;
import org.tikv.service.failsafe.CircuitBreaker;
import org.tikv.service.failsafe.CircuitBreakerImpl;
import org.tikv.txn.KVClient;
import org.tikv.txn.TxnKVClient;

/**
 * TiSession is the holder for PD Client, Store pdClient and PD Cache All sessions share common
 * region store connection pool but separated PD conn and cache for better concurrency
 *
 * <p>TiSession is thread-safe but it's also recommended to have multiple session avoiding lock
 * contention
 */
public class TiSession implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TiSession.class);
  private static final Map<String, TiSession> sessionCachedMap = new HashMap<>();
  private final TiConfiguration conf;
  private final RequestKeyCodec keyCodec;
  private final ChannelFactory channelFactory;
  // below object creation is either heavy or making connection (pd), pending for lazy loading
  private volatile PDClient client;
  private volatile Catalog catalog;
  private volatile ExecutorService indexScanThreadPool;
  private volatile ExecutorService tableScanThreadPool;
  private volatile ExecutorService batchGetThreadPool;
  private volatile ExecutorService batchPutThreadPool;
  private volatile ExecutorService batchDeleteThreadPool;
  private volatile ExecutorService batchScanThreadPool;
  private volatile ExecutorService deleteRangeThreadPool;
  private volatile RegionManager regionManager;
  private final boolean enableGrpcForward;
  private volatile RegionStoreClient.RegionStoreClientBuilder clientBuilder;
  private volatile ImporterStoreClient.ImporterStoreClientBuilder importerClientBuilder;
  private volatile boolean isClosed = false;
  private volatile SwitchTiKVModeClient switchTiKVModeClient;
  private final MetricsServer metricsServer;
  private final CircuitBreaker circuitBreaker;
  private static final int MAX_SPLIT_REGION_STACK_DEPTH = 6;

  static {
    logger.info("Welcome to TiKV Java Client {}", getVersionInfo());
  }

  private static class VersionInfo {

    private final String buildVersion;
    private final String commitHash;

    public VersionInfo(String buildVersion, String commitHash) {
      this.buildVersion = buildVersion;
      this.commitHash = commitHash;
    }

    @Override
    public String toString() {
      return buildVersion + "@" + commitHash;
    }
  }

  public TiSession(TiConfiguration conf) {
    // may throw org.tikv.common.MetricsServer  - http server not up
    // put it at the beginning of this function to avoid unclosed Thread
    this.metricsServer = MetricsServer.getInstance(conf);

    this.conf = conf;

    if (conf.getApiVersion().isV1()) {
      if (conf.isRawKVMode()) {
        keyCodec = new RequestKeyV1RawCodec();
      } else {
        keyCodec = new RequestKeyV1TxnCodec();
      }
    } else {
      if (conf.isRawKVMode()) {
        keyCodec = new RequestKeyV2RawCodec();
      } else {
        keyCodec = new RequestKeyV2TxnCodec();
      }
    }

    if (conf.isTlsEnable()) {
      if (conf.isJksEnable()) {
        this.channelFactory =
            new ChannelFactory(
                conf.getMaxFrameSize(),
                conf.getKeepaliveTime(),
                conf.getKeepaliveTimeout(),
                conf.getIdleTimeout(),
                conf.getConnRecycleTimeInSeconds(),
                conf.getCertReloadIntervalInSeconds(),
                conf.getJksKeyPath(),
                conf.getJksKeyPassword(),
                conf.getJksTrustPath(),
                conf.getJksTrustPassword());
      } else {
        this.channelFactory =
            new ChannelFactory(
                conf.getMaxFrameSize(),
                conf.getKeepaliveTime(),
                conf.getKeepaliveTimeout(),
                conf.getIdleTimeout(),
                conf.getConnRecycleTimeInSeconds(),
                conf.getCertReloadIntervalInSeconds(),
                conf.getTrustCertCollectionFile(),
                conf.getKeyCertChainFile(),
                conf.getKeyFile());
      }
    } else {
      this.channelFactory =
          new ChannelFactory(
              conf.getMaxFrameSize(),
              conf.getKeepaliveTime(),
              conf.getKeepaliveTimeout(),
              conf.getIdleTimeout());
    }

    this.client = PDClient.createRaw(conf, keyCodec, channelFactory);
    this.enableGrpcForward = conf.getEnableGrpcForward();
    if (this.enableGrpcForward) {
      logger.info("enable grpc forward for high available");
    }
    if (conf.isWarmUpEnable() && conf.isRawKVMode()) {
      warmup();
    }
    this.circuitBreaker = new CircuitBreakerImpl(conf, client.getClusterId());
    logger.info(
        "TiSession initialized in "
            + conf.getKvMode()
            + " mode in API version: "
            + conf.getApiVersion());
  }

  private static VersionInfo getVersionInfo() {
    VersionInfo info;
    try {
      final Properties properties = new Properties();
      properties.load(TiSession.class.getClassLoader().getResourceAsStream("git.properties"));
      String version = properties.getProperty("git.build.version");
      String commitHash = properties.getProperty("git.commit.id.full");
      info = new VersionInfo(version, commitHash);
    } catch (Exception e) {
      logger.info("Fail to read package info: " + e.getMessage());
      info = new VersionInfo("unknown", "unknown");
    }
    return info;
  }

  @VisibleForTesting
  public synchronized void warmup() {
    long warmUpStartTime = System.nanoTime();
    BackOffer backOffer = ConcreteBackOffer.newRawKVBackOff(getPDClient().getClusterId());
    try {
      // let JVM ClassLoader load gRPC error related classes
      // this operation may cost 100ms
      Errorpb.Error.newBuilder().setNotLeader(Errorpb.NotLeader.newBuilder().build()).build();

      this.client = getPDClient();
      this.regionManager = getRegionManager();
      List<Metapb.Store> stores = this.client.getAllStores(backOffer);
      // warm up store cache
      for (Metapb.Store store : stores) {
        this.regionManager.updateStore(
            null, new TiStore(this.client.getStore(backOffer, store.getId())));
      }

      // use scan region to load region cache with limit
      ByteString startKey = ByteString.EMPTY;
      do {
        List<Pdpb.Region> regions =
            regionManager.scanRegions(
                backOffer, startKey, ByteString.EMPTY, conf.getScanRegionsLimit());
        if (regions == null || regions.isEmpty()) {
          // something went wrong, but the warm-up process could continue
          break;
        }
        for (Pdpb.Region region : regions) {
          regionManager.insertRegionToCache(
              regionManager.createRegion(region.getRegion(), backOffer));
        }
        startKey = regions.get(regions.size() - 1).getRegion().getEndKey();
      } while (!startKey.isEmpty());

      try (RawKVClient rawKVClient = createRawClient()) {
        ByteString exampleKey = ByteString.EMPTY;
        Optional<ByteString> prev = rawKVClient.get(exampleKey);
        if (prev.isPresent()) {
          rawKVClient.delete(exampleKey);
          rawKVClient.putIfAbsent(exampleKey, prev.get());
          rawKVClient.put(exampleKey, prev.get());
        } else {
          rawKVClient.putIfAbsent(exampleKey, ByteString.EMPTY);
          rawKVClient.put(exampleKey, ByteString.EMPTY);
          rawKVClient.delete(exampleKey);
        }
      }
    } catch (Exception e) {
      // ignore error
      logger.info("warm up fails, ignored ", e);
    } finally {
      logger.info(
          String.format(
              "warm up duration %d ms", (System.nanoTime() - warmUpStartTime) / 1_000_000));
    }
  }

  @VisibleForTesting
  public static TiSession create(TiConfiguration conf) {
    return new TiSession(conf);
  }

  @Deprecated
  public static TiSession getInstance(TiConfiguration conf) {
    synchronized (sessionCachedMap) {
      String key = conf.getPdAddrsString();
      if (sessionCachedMap.containsKey(key)) {
        return sessionCachedMap.get(key);
      }

      TiSession newSession = new TiSession(conf);
      sessionCachedMap.put(key, newSession);
      return newSession;
    }
  }

  public RawKVClient createRawClient() {
    checkIsClosed();

    return new RawKVClient(this, this.getRegionStoreClientBuilder());
  }

  public SmartRawKVClient createSmartRawClient() {
    RawKVClient rawKVClient = createRawClient();
    return new SmartRawKVClient(rawKVClient, circuitBreaker);
  }

  public KVClient createKVClient() {
    checkIsClosed();

    return new KVClient(this.conf, this.getRegionStoreClientBuilder(), this);
  }

  public TxnKVClient createTxnClient() {
    checkIsClosed();

    return new TxnKVClient(conf, this.getRegionStoreClientBuilder(), this.getPDClient());
  }

  public RegionStoreClient.RegionStoreClientBuilder getRegionStoreClientBuilder() {
    checkIsClosed();

    if (this.clientBuilder != null) {
      return this.clientBuilder;
    }

    // lazily create the clientBuilder for the current TiSession
    synchronized (this) {
      if (this.clientBuilder == null) {
        this.clientBuilder =
            new RegionStoreClient.RegionStoreClientBuilder(
                this.conf, this.channelFactory, this.getRegionManager(), this.getPDClient());
      }
    }
    return this.clientBuilder;
  }

  public ImporterStoreClient.ImporterStoreClientBuilder getImporterRegionStoreClientBuilder() {
    checkIsClosed();

    ImporterStoreClient.ImporterStoreClientBuilder res = importerClientBuilder;
    if (res == null) {
      synchronized (this) {
        if (importerClientBuilder == null) {
          if (conf.isTxnKVMode()) {
            importerClientBuilder =
                new ImporterStoreClient.ImporterStoreClientBuilder<
                    ImportSstpb.WriteRequest, ImportSstpb.WriteRequest>(
                    conf, this.channelFactory, this.getRegionManager(), this.getPDClient());
          } else {
            importerClientBuilder =
                new ImporterStoreClient.ImporterStoreClientBuilder<
                    ImportSstpb.RawWriteRequest, ImportSstpb.RawWriteResponse>(
                    conf, this.channelFactory, this.getRegionManager(), this.getPDClient());
          }
        }
        res = importerClientBuilder;
      }
    }
    return res;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public TiTimestamp getTimestamp() {
    checkIsClosed();

    return getPDClient()
        .getTimestamp(ConcreteBackOffer.newTsoBackOff(getPDClient().getClusterId()));
  }

  public Snapshot createSnapshot() {
    checkIsClosed();

    return new Snapshot(getTimestamp(), this);
  }

  public Snapshot createSnapshot(TiTimestamp ts) {
    checkIsClosed();

    return new Snapshot(ts, this);
  }

  public PDClient getPDClient() {
    checkIsClosed();

    PDClient res = client;
    if (res == null) {
      synchronized (this) {
        if (client == null) {
          client = PDClient.createRaw(this.getConf(), keyCodec, channelFactory);
        }
        res = client;
      }
    }
    return res;
  }

  public Catalog getCatalog() {
    checkIsClosed();

    Catalog res = catalog;
    if (res == null) {
      synchronized (this) {
        if (catalog == null) {
          catalog = new Catalog(this::createSnapshot, conf.ifShowRowId(), conf.getDBPrefix());
        }
        res = catalog;
      }
    }
    return res;
  }

  public RegionManager getRegionManager() {
    checkIsClosed();

    RegionManager res = regionManager;
    if (res == null) {
      synchronized (this) {
        if (regionManager == null) {
          regionManager = new RegionManager(getConf(), getPDClient(), this.channelFactory);
        }
        res = regionManager;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForIndexScan() {
    checkIsClosed();

    ExecutorService res = indexScanThreadPool;
    if (res == null) {
      synchronized (this) {
        if (indexScanThreadPool == null) {
          indexScanThreadPool =
              Executors.newFixedThreadPool(
                  conf.getIndexScanConcurrency(),
                  new ThreadFactoryBuilder()
                      .setNameFormat("index-scan-pool-%d")
                      .setDaemon(true)
                      .build());
        }
        res = indexScanThreadPool;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForTableScan() {
    checkIsClosed();

    ExecutorService res = tableScanThreadPool;
    if (res == null) {
      synchronized (this) {
        if (tableScanThreadPool == null) {
          tableScanThreadPool =
              Executors.newFixedThreadPool(
                  conf.getTableScanConcurrency(),
                  new ThreadFactoryBuilder().setDaemon(true).build());
        }
        res = tableScanThreadPool;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForBatchPut() {
    checkIsClosed();

    ExecutorService res = batchPutThreadPool;
    if (res == null) {
      synchronized (this) {
        if (batchPutThreadPool == null) {
          batchPutThreadPool =
              Executors.newFixedThreadPool(
                  conf.getBatchPutConcurrency(),
                  new ThreadFactoryBuilder()
                      .setNameFormat("batchPut-thread-%d")
                      .setDaemon(true)
                      .build());
        }
        res = batchPutThreadPool;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForBatchGet() {
    checkIsClosed();

    ExecutorService res = batchGetThreadPool;
    if (res == null) {
      synchronized (this) {
        if (batchGetThreadPool == null) {
          batchGetThreadPool =
              Executors.newFixedThreadPool(
                  conf.getBatchGetConcurrency(),
                  new ThreadFactoryBuilder()
                      .setNameFormat("batchGet-thread-%d")
                      .setDaemon(true)
                      .build());
        }
        res = batchGetThreadPool;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForBatchDelete() {
    checkIsClosed();

    ExecutorService res = batchDeleteThreadPool;
    if (res == null) {
      synchronized (this) {
        if (batchDeleteThreadPool == null) {
          batchDeleteThreadPool =
              Executors.newFixedThreadPool(
                  conf.getBatchDeleteConcurrency(),
                  new ThreadFactoryBuilder()
                      .setNameFormat("batchDelete-thread-%d")
                      .setDaemon(true)
                      .build());
        }
        res = batchDeleteThreadPool;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForBatchScan() {
    checkIsClosed();

    ExecutorService res = batchScanThreadPool;
    if (res == null) {
      synchronized (this) {
        if (batchScanThreadPool == null) {
          batchScanThreadPool =
              Executors.newFixedThreadPool(
                  conf.getBatchScanConcurrency(),
                  new ThreadFactoryBuilder()
                      .setNameFormat("batchScan-thread-%d")
                      .setDaemon(true)
                      .build());
        }
        res = batchScanThreadPool;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForDeleteRange() {
    checkIsClosed();

    ExecutorService res = deleteRangeThreadPool;
    if (res == null) {
      synchronized (this) {
        if (deleteRangeThreadPool == null) {
          deleteRangeThreadPool =
              Executors.newFixedThreadPool(
                  conf.getDeleteRangeConcurrency(),
                  new ThreadFactoryBuilder()
                      .setNameFormat("deleteRange-thread-%d")
                      .setDaemon(true)
                      .build());
        }
        res = deleteRangeThreadPool;
      }
    }
    return res;
  }

  @VisibleForTesting
  public ChannelFactory getChannelFactory() {
    checkIsClosed();

    return channelFactory;
  }

  /**
   * SwitchTiKVModeClient is used for SST Ingest.
   *
   * @return a SwitchTiKVModeClient
   */
  public SwitchTiKVModeClient getSwitchTiKVModeClient() {
    checkIsClosed();

    SwitchTiKVModeClient res = switchTiKVModeClient;
    if (res == null) {
      synchronized (this) {
        if (switchTiKVModeClient == null) {
          switchTiKVModeClient =
              new SwitchTiKVModeClient(getPDClient(), getImporterRegionStoreClientBuilder());
        }
        res = switchTiKVModeClient;
      }
    }
    return res;
  }

  /**
   * split region and scatter
   *
   * @param splitKeys
   * @param splitRegionBackoffMS
   * @param scatterRegionBackoffMS
   * @param scatterWaitMS
   */
  public void splitRegionAndScatter(
      List<byte[]> splitKeys,
      int splitRegionBackoffMS,
      int scatterRegionBackoffMS,
      int scatterWaitMS) {
    checkIsClosed();

    logger.info(String.format("split key's size is %d", splitKeys.size()));
    long startMS = System.currentTimeMillis();

    // split region
    List<Metapb.Region> newRegions =
        splitRegion(
            splitKeys
                .stream()
                .map(k -> Key.toRawKey(k).toByteString())
                .collect(Collectors.toList()),
            ConcreteBackOffer.newCustomBackOff(splitRegionBackoffMS, getPDClient().getClusterId()));

    // scatter region
    for (Metapb.Region newRegion : newRegions) {
      try {
        getPDClient()
            .scatterRegion(
                newRegion,
                ConcreteBackOffer.newCustomBackOff(
                    scatterRegionBackoffMS, getPDClient().getClusterId()));
      } catch (Exception e) {
        logger.warn(String.format("failed to scatter region: %d", newRegion.getId()), e);
      }
    }

    // wait scatter region finish
    if (scatterWaitMS > 0) {
      logger.info("start to wait scatter region finish");
      long scatterRegionStartMS = System.currentTimeMillis();
      for (Metapb.Region newRegion : newRegions) {
        long remainMS = (scatterRegionStartMS + scatterWaitMS) - System.currentTimeMillis();
        if (remainMS <= 0) {
          logger.warn("wait scatter region timeout");
          return;
        }
        getPDClient()
            .waitScatterRegionFinish(
                newRegion,
                ConcreteBackOffer.newCustomBackOff((int) remainMS, getPDClient().getClusterId()));
      }
    } else {
      logger.info("skip to wait scatter region finish");
    }

    long endMS = System.currentTimeMillis();
    logger.info("splitRegionAndScatter cost {} seconds", (endMS - startMS) / 1000);
  }

  /**
   * split region and scatter
   *
   * @param splitKeys
   */
  public void splitRegionAndScatter(List<byte[]> splitKeys) {
    checkIsClosed();

    int splitRegionBackoffMS = BackOffer.SPLIT_REGION_BACKOFF;
    int scatterRegionBackoffMS = BackOffer.SCATTER_REGION_BACKOFF;
    int scatterWaitMS = conf.getScatterWaitSeconds() * 1000;
    splitRegionAndScatter(splitKeys, splitRegionBackoffMS, scatterRegionBackoffMS, scatterWaitMS);
  }

  private List<Metapb.Region> splitRegion(List<ByteString> splitKeys, BackOffer backOffer) {
    return splitRegion(splitKeys, backOffer, 1);
  }

  private List<Metapb.Region> splitRegion(
      List<ByteString> splitKeys, BackOffer backOffer, int depth) {
    List<Metapb.Region> regions = new ArrayList<>();

    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(getRegionManager(), splitKeys, backOffer);
    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {

      Pair<TiRegion, TiStore> pair =
          getRegionManager().getRegionStorePairByKey(entry.getKey().getStartKey());
      TiRegion region = pair.first;
      TiStore store = pair.second;
      List<ByteString> splits =
          entry
              .getValue()
              .stream()
              .filter(k -> !k.equals(region.getStartKey()) && !k.equals(region.getEndKey()))
              .collect(Collectors.toList());

      if (splits.isEmpty()) {
        logger.warn(
            "split key equal to region start key or end key. Region splitting is not needed.");
      } else {
        logger.info("start to split region id={}, split size={}", region.getId(), splits.size());
        List<Metapb.Region> newRegions;
        try {
          newRegions = getRegionStoreClientBuilder().build(region, store).splitRegion(splits);
          // invalidate old region
          getRegionManager().invalidateRegion(region);
        } catch (final TiKVException e) {
          // retry
          logger.warn("ReSplitting ranges for splitRegion", e);
          getRegionManager().invalidateRegion(region);
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          if (depth >= MAX_SPLIT_REGION_STACK_DEPTH) {
            logger.warn(
                String.format(
                    "Skip split region because MAX_SPLIT_REGION_STACK_DEPTH(%d) reached!",
                    MAX_SPLIT_REGION_STACK_DEPTH));
            newRegions = new ArrayList<>();
          } else {
            newRegions = splitRegion(splits, backOffer, depth + 1);
          }
        }
        logger.info("region id={}, new region size={}", region.getId(), newRegions.size());
        regions.addAll(newRegions);
      }
    }

    logger.info("splitRegion: return region size={}", regions.size());
    return regions;
  }

  private void checkIsClosed() {
    if (isClosed) {
      throw new RuntimeException("this TiSession is closed!");
    }
  }

  public synchronized void closeAwaitTermination(long timeoutMS) throws Exception {
    shutdown(false);

    long startMS = System.currentTimeMillis();
    while (true) {
      if (isTerminatedExecutorServices()) {
        cleanAfterTerminated();
        return;
      }

      if (System.currentTimeMillis() - startMS > timeoutMS) {
        shutdown(true);
        return;
      }
      Thread.sleep(500);
    }
  }

  @Override
  public synchronized void close() throws Exception {
    shutdown(true);
  }

  private synchronized void shutdown(boolean now) throws Exception {
    if (!isClosed) {
      isClosed = true;
      synchronized (sessionCachedMap) {
        sessionCachedMap.remove(conf.getPdAddrsString());
      }

      if (metricsServer != null) {
        metricsServer.close();
      }

      if (circuitBreaker != null) {
        circuitBreaker.close();
      }
    }

    if (now) {
      shutdownNowExecutorServices();
      cleanAfterTerminated();
    } else {
      shutdownExecutorServices();
    }
  }

  private synchronized void cleanAfterTerminated() throws InterruptedException {
    if (regionManager != null) {
      regionManager.close();
    }
    if (client != null) {
      client.close();
    }
    if (catalog != null) {
      catalog.close();
    }

    if (switchTiKVModeClient != null) {
      switchTiKVModeClient.stopKeepTiKVToImportMode();
    }
  }

  private List<ExecutorService> getExecutorServices() {
    List<ExecutorService> executorServiceList = new ArrayList<>();
    if (tableScanThreadPool != null) {
      executorServiceList.add(tableScanThreadPool);
    }
    if (indexScanThreadPool != null) {
      executorServiceList.add(indexScanThreadPool);
    }
    if (batchGetThreadPool != null) {
      executorServiceList.add(batchGetThreadPool);
    }
    if (batchPutThreadPool != null) {
      executorServiceList.add(batchPutThreadPool);
    }
    if (batchDeleteThreadPool != null) {
      executorServiceList.add(batchDeleteThreadPool);
    }
    if (batchScanThreadPool != null) {
      executorServiceList.add(batchScanThreadPool);
    }
    if (deleteRangeThreadPool != null) {
      executorServiceList.add(deleteRangeThreadPool);
    }
    return executorServiceList;
  }

  private void shutdownExecutorServices() {
    for (ExecutorService executorService : getExecutorServices()) {
      executorService.shutdown();
    }
  }

  private void shutdownNowExecutorServices() {
    for (ExecutorService executorService : getExecutorServices()) {
      executorService.shutdownNow();
    }
  }

  private boolean isTerminatedExecutorServices() {
    for (ExecutorService executorService : getExecutorServices()) {
      if (!executorService.isTerminated()) {
        return false;
      }
    }
    return true;
  }
}
