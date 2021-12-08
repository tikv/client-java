/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.*;
import org.tikv.kvproto.Metapb;
import org.tikv.raw.RawKVClient;
import org.tikv.txn.KVClient;
import org.tikv.txn.TxnKVClient;

/**
 * TiSession is the holder for PD Client, Store pdClient and PD Cache All sessions share common
 * region store connection pool but separated PD conn and cache for better concurrency TiSession is
 * thread-safe but it's also recommended to have multiple session avoiding lock contention
 */
public class TiSession implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TiSession.class);
  private static final Map<String, TiSession> sessionCachedMap = new HashMap<>();
  private final TiConfiguration conf;
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
  private volatile boolean enableGrpcForward;
  private volatile RegionStoreClient.RegionStoreClientBuilder clientBuilder;
  private volatile boolean isClosed = false;
  private MetricsServer metricsServer;

  public TiSession(TiConfiguration conf) {
    // may throw org.tikv.common.MetricsServer  - http server not up
    // put it at the beginning of this function to avoid unclosed Thread
    this.metricsServer = MetricsServer.getInstance(conf);

    this.conf = conf;
    this.channelFactory = new ChannelFactory(conf.getMaxFrameSize());
    this.client = PDClient.createRaw(conf, channelFactory);
    this.enableGrpcForward = conf.getEnableGrpcForward();
    if (this.enableGrpcForward) {
      logger.info("enable grpc forward for high available");
    }
    logger.info("TiSession initialized in " + conf.getKvMode() + " mode");
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

    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(conf, channelFactory, this.getRegionManager(), client);
    RawKVClient rawClient = new RawKVClient(this, builder);

    // Warm up raw client to avoid a slow first call.
    rawClient.get(ByteString.EMPTY);

    return rawClient;
  }

  public KVClient createKVClient() {
    checkIsClosed();

    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(conf, channelFactory, this.getRegionManager(), client);
    return new KVClient(conf, builder);
  }

  public TxnKVClient createTxnClient() {
    checkIsClosed();

    return new TxnKVClient(conf, this.getRegionStoreClientBuilder(), this.getPDClient());
  }

  public RegionStoreClient.RegionStoreClientBuilder getRegionStoreClientBuilder() {
    checkIsClosed();

    RegionStoreClient.RegionStoreClientBuilder res = clientBuilder;
    if (res == null) {
      synchronized (this) {
        if (clientBuilder == null) {
          clientBuilder =
              new RegionStoreClient.RegionStoreClientBuilder(
                  conf, this.channelFactory, this.getRegionManager(), this.getPDClient());
        }
        res = clientBuilder;
      }
    }
    return res;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public TiTimestamp getTimestamp() {
    checkIsClosed();

    return getPDClient().getTimestamp(ConcreteBackOffer.newTsoBackOff());
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
          client = PDClient.createRaw(this.getConf(), channelFactory);
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
   * split region and scatter
   *
   * @param splitKeys
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
                .map(k -> Key.toRawKey(k).next().toByteString())
                .collect(Collectors.toList()),
            ConcreteBackOffer.newCustomBackOff(splitRegionBackoffMS));

    // scatter region
    for (Metapb.Region newRegion : newRegions) {
      try {
        getPDClient()
            .scatterRegion(newRegion, ConcreteBackOffer.newCustomBackOff(scatterRegionBackoffMS));
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
            .waitScatterRegionFinish(newRegion, ConcreteBackOffer.newCustomBackOff((int) remainMS));
      }
    } else {
      logger.info("skip to wait scatter region finish");
    }

    long endMS = System.currentTimeMillis();
    logger.info("splitRegionAndScatter cost {} seconds", (endMS - startMS) / 1000);
  }

  private List<Metapb.Region> splitRegion(List<ByteString> splitKeys, BackOffer backOffer) {
    List<Metapb.Region> regions = new ArrayList<>();

    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(regionManager, splitKeys, backOffer);
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
        } catch (final TiKVException e) {
          // retry
          logger.warn("ReSplitting ranges for splitRegion", e);
          clientBuilder.getRegionManager().invalidateRegion(region);
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          newRegions = splitRegion(splits, backOffer);
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
