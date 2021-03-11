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
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.policy.RetryPolicy;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
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
  private Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;
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
  private volatile RegionStoreClient.RegionStoreClientBuilder clientBuilder;
  private boolean isClosed = false;
  private HTTPServer server;
  private CollectorRegistry collectorRegistry;

  public TiSession(TiConfiguration conf) {
    this.conf = conf;
    this.channelFactory = new ChannelFactory(conf.getMaxFrameSize());
    this.client = PDClient.createRaw(conf, channelFactory);
    if (conf.isMetricsEnable()) {
      try {
        this.collectorRegistry = new CollectorRegistry();
        this.collectorRegistry.register(RawKVClient.RAW_REQUEST_LATENCY);
        this.collectorRegistry.register(RawKVClient.RAW_REQUEST_FAILURE);
        this.collectorRegistry.register(RawKVClient.RAW_REQUEST_SUCCESS);
        this.collectorRegistry.register(RegionStoreClient.GRPC_RAW_REQUEST_LATENCY);
        this.collectorRegistry.register(RetryPolicy.GRPC_SINGLE_REQUEST_LATENCY);
        this.collectorRegistry.register(RegionManager.GET_REGION_BY_KEY_REQUEST_LATENCY);
        this.collectorRegistry.register(PDClient.PD_GET_REGION_BY_KEY_REQUEST_LATENCY);
        this.server =
            new HTTPServer(
                new InetSocketAddress(conf.getMetricsPort()), this.collectorRegistry, true);
        logger.info("http server is up " + this.server.getPort());
      } catch (Exception e) {
        logger.error("http server not up");
        throw new RuntimeException(e);
      }
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
    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(conf, channelFactory, this.getRegionManager(), client);
    return new RawKVClient(this, builder);
  }

  public KVClient createKVClient() {
    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(conf, channelFactory, this.getRegionManager(), client);
    return new KVClient(conf, builder);
  }

  public TxnKVClient createTxnClient() {
    return new TxnKVClient(conf, this.getRegionStoreClientBuilder(), this.getPDClient());
  }

  public RegionStoreClient.RegionStoreClientBuilder getRegionStoreClientBuilder() {
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
    return getPDClient().getTimestamp(ConcreteBackOffer.newTsoBackOff());
  }

  public Snapshot createSnapshot() {
    return new Snapshot(getTimestamp(), this);
  }

  public Snapshot createSnapshot(TiTimestamp ts) {
    return new Snapshot(ts, this);
  }

  public PDClient getPDClient() {
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

  public synchronized RegionManager getRegionManager() {
    RegionManager res = regionManager;
    if (res == null) {
      synchronized (this) {
        if (regionManager == null) {
          regionManager = new RegionManager(getPDClient(), this.cacheInvalidateCallback);
        }
        res = regionManager;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForIndexScan() {
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
    return channelFactory;
  }

  public CollectorRegistry getCollectorRegistry() {
    return collectorRegistry;
  }

  /**
   * This is used for setting call back function to invalidate cache information
   *
   * @param callBackFunc callback function
   */
  public void injectCallBackFunc(Function<CacheInvalidateEvent, Void> callBackFunc) {
    this.cacheInvalidateCallback = callBackFunc;
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
    logger.info(String.format("split key's size is %d", splitKeys.size()));
    long startMS = System.currentTimeMillis();

    // split region
    List<TiRegion> newRegions =
        splitRegion(
            splitKeys
                .stream()
                .map(k -> Key.toRawKey(k).next().toByteString())
                .collect(Collectors.toList()),
            ConcreteBackOffer.newCustomBackOff(splitRegionBackoffMS));

    // scatter region
    for (TiRegion newRegion : newRegions) {
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
      for (TiRegion newRegion : newRegions) {
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

  private List<TiRegion> splitRegion(List<ByteString> splitKeys, BackOffer backOffer) {
    List<TiRegion> regions = new ArrayList<>();

    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(regionManager, splitKeys, backOffer);
    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {

      Pair<TiRegion, Metapb.Store> pair =
          getRegionManager().getRegionStorePairByKey(entry.getKey().getStartKey());
      TiRegion region = pair.first;
      Metapb.Store store = pair.second;
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
        List<TiRegion> newRegions;
        try {
          newRegions = getRegionStoreClientBuilder().build(region, store).splitRegion(splits);
        } catch (final TiKVException e) {
          // retry
          logger.warn("ReSplitting ranges for splitRegion", e);
          clientBuilder.getRegionManager().invalidateRegion(region.getId());
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

  @Override
  public synchronized void close() throws Exception {
    if (isClosed) {
      logger.warn("this TiSession is already closed!");
      return;
    }

    if (server != null) {
      server.stop();
      logger.info("Metrics server on " + server.getPort() + " is stopped");
    }

    isClosed = true;
    synchronized (sessionCachedMap) {
      sessionCachedMap.remove(conf.getPdAddrsString());
    }

    if (tableScanThreadPool != null) {
      tableScanThreadPool.shutdownNow();
    }
    if (indexScanThreadPool != null) {
      indexScanThreadPool.shutdownNow();
    }
    if (batchGetThreadPool != null) {
      batchGetThreadPool.shutdownNow();
    }
    if (batchPutThreadPool != null) {
      batchPutThreadPool.shutdownNow();
    }
    if (batchDeleteThreadPool != null) {
      batchDeleteThreadPool.shutdownNow();
    }
    if (batchScanThreadPool != null) {
      batchScanThreadPool.shutdownNow();
    }
    if (deleteRangeThreadPool != null) {
      deleteRangeThreadPool.shutdownNow();
    }
    if (client != null) {
      getPDClient().close();
    }
    if (catalog != null) {
      getCatalog().close();
    }
  }
}
