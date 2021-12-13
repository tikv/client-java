/*
 *
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
 *
 */

package org.tikv.common.region;

import static org.tikv.common.codec.KeyUtils.formatBytesUTF8;

import com.google.protobuf.ByteString;
import io.prometheus.client.Histogram;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.InvalidStoreException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.StoreState;

@SuppressWarnings("UnstableApiUsage")
public class RegionManager {
  private static final Logger logger = LoggerFactory.getLogger(RegionManager.class);
  public static final Histogram GET_REGION_BY_KEY_REQUEST_LATENCY =
      Histogram.build()
          .name("client_java_get_region_by_requests_latency")
          .help("getRegionByKey request latency.")
          .register();

  // TODO: the region cache logic need rewrite.
  // https://github.com/pingcap/tispark/issues/1170
  private final RegionCache cache;
  private final ReadOnlyPDClient pdClient;
  private final TiConfiguration conf;
  private final ScheduledExecutorService executor;
  private final StoreHealthyChecker storeChecker;

  public RegionManager(
      TiConfiguration conf, ReadOnlyPDClient pdClient, ChannelFactory channelFactory) {
    this.cache = new RegionCache();
    this.pdClient = pdClient;
    this.conf = conf;
    long period = conf.getHealthCheckPeriodDuration();
    StoreHealthyChecker storeChecker =
        new StoreHealthyChecker(
            channelFactory, pdClient, this.cache, conf.getGrpcHealthCheckTimeout());
    this.storeChecker = storeChecker;
    this.executor = Executors.newScheduledThreadPool(1);
    this.executor.scheduleAtFixedRate(storeChecker, period, period, TimeUnit.MILLISECONDS);
  }

  public RegionManager(TiConfiguration conf, ReadOnlyPDClient pdClient) {
    this.cache = new RegionCache();
    this.pdClient = pdClient;
    this.conf = conf;
    this.storeChecker = null;
    this.executor = null;
  }

  public synchronized void close() {
    if (this.executor != null) {
      this.executor.shutdownNow();
    }
  }

  public ReadOnlyPDClient getPDClient() {
    return this.pdClient;
  }

  public void invalidateAll() {
    cache.invalidateAll();
  }

  public TiRegion getRegionByKey(ByteString key) {
    return getRegionByKey(key, defaultBackOff());
  }

  public TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
    Histogram.Timer requestTimer = GET_REGION_BY_KEY_REQUEST_LATENCY.startTimer();
    SlowLogSpan slowLogSpan = backOffer.getSlowLog().start("getRegionByKey");
    TiRegion region = cache.getRegionByKey(key, backOffer);
    try {
      if (region == null) {
        logger.debug("Key not found in keyToRegionIdCache:" + formatBytesUTF8(key));
        Pair<Metapb.Region, Metapb.Peer> regionAndLeader = pdClient.getRegionByKey(backOffer, key);
        region =
            cache.putRegion(createRegion(regionAndLeader.first, regionAndLeader.second, backOffer));
      }
    } catch (Exception e) {
      return null;
    } finally {
      requestTimer.observeDuration();
      slowLogSpan.end();
    }

    return region;
  }

  @Deprecated
  // Do not use GetRegionByID when retrying request.
  //
  //   A,B |_______|_____|
  //   A   |_____________|
  // Consider region A, B. After merge of (A, B) -> A, region ID B does not exist.
  // This request is unrecoverable.
  public TiRegion getRegionById(long regionId) {
    BackOffer backOffer = defaultBackOff();
    TiRegion region = cache.getRegionById(regionId);
    if (region == null) {
      Pair<Metapb.Region, Metapb.Peer> regionAndLeader =
          pdClient.getRegionByID(backOffer, regionId);
      region = createRegion(regionAndLeader.first, regionAndLeader.second, backOffer);
      return cache.putRegion(region);
    }
    return region;
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(ByteString key, BackOffer backOffer) {
    return getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(ByteString key) {
    return getRegionStorePairByKey(key, TiStoreType.TiKV);
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(ByteString key, TiStoreType storeType) {
    return getRegionStorePairByKey(key, storeType, defaultBackOff());
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(
      ByteString key, TiStoreType storeType, BackOffer backOffer) {
    TiRegion region = getRegionByKey(key, backOffer);
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region);
    }

    TiStore store = null;
    if (storeType == TiStoreType.TiKV) {
      Peer peer = region.getCurrentReplica();
      store = getStoreById(peer.getStoreId(), backOffer);
    } else {
      outerLoop:
      for (Peer peer : region.getLearnerList()) {
        TiStore s = getStoreById(peer.getStoreId(), backOffer);
        for (Metapb.StoreLabel label : s.getStore().getLabelsList()) {
          if (label.getKey().equals(storeType.getLabelKey())
              && label.getValue().equals(storeType.getLabelValue())) {
            store = s;
            break outerLoop;
          }
        }
      }
      if (store == null) {
        // clear the region cache, so we may get the learner peer next time
        cache.invalidateRegion(region);
      }
    }
    return Pair.create(region, store);
  }

  public TiRegion createRegion(Metapb.Region region, BackOffer backOffer) {
    List<Metapb.Peer> peers = region.getPeersList();
    List<TiStore> stores = getRegionStore(peers, backOffer);
    return new TiRegion(conf, region, null, peers, stores);
  }

  private TiRegion createRegion(Metapb.Region region, Metapb.Peer leader, BackOffer backOffer) {
    List<Metapb.Peer> peers = region.getPeersList();
    List<TiStore> stores = getRegionStore(peers, backOffer);
    return new TiRegion(conf, region, leader, peers, stores);
  }

  private List<TiStore> getRegionStore(List<Metapb.Peer> peers, BackOffer backOffer) {
    return peers
        .stream()
        .map(p -> getStoreById(p.getStoreId(), backOffer))
        .collect(Collectors.toList());
  }

  private TiStore getStoreByIdWithBackOff(long id, BackOffer backOffer) {
    try {
      TiStore store = cache.getStoreById(id);
      if (store == null) {
        store = new TiStore(pdClient.getStore(backOffer, id));
      } else {
        return store;
      }
      // if we did not get store info from pd, remove store from cache
      if (store.getStore() == null) {
        logger.warn(String.format("failed to get store %d from pd", id));
        return null;
      }
      // if the store is already tombstone, remove store from cache
      if (store.getStore().getState().equals(StoreState.Tombstone)) {
        logger.warn(String.format("store %d is tombstone", id));
        return null;
      }
      if (cache.putStore(id, store) && storeChecker != null) {
        storeChecker.scheduleStoreHealthCheck(store);
      }
      return store;
    } catch (Exception e) {
      throw new GrpcException(e);
    }
  }

  public TiStore getStoreById(long id) {
    return getStoreById(id, defaultBackOff());
  }

  public TiStore getStoreById(long id, BackOffer backOffer) {
    TiStore store = getStoreByIdWithBackOff(id, backOffer);
    if (store == null) {
      logger.warn(String.format("failed to fetch store %d, the store may be missing", id));
      cache.clearAll();
      throw new InvalidStoreException(id);
    }
    return store;
  }

  public void onRegionStale(TiRegion region) {
    cache.invalidateRegion(region);
  }

  public TiRegion updateLeader(TiRegion region, long storeId) {
    if (region.getLeader().getStoreId() == storeId) {
      return region;
    }
    TiRegion newRegion = region.switchPeer(storeId);
    if (cache.updateRegion(region, newRegion)) {
      return newRegion;
    }
    // failed to switch leader, possibly region is outdated, we need to drop region cache from
    // regionCache
    logger.warn("Cannot find peer when updating leader (" + region.getId() + "," + storeId + ")");
    return null;
  }

  public synchronized void updateStore(TiStore oldStore, TiStore newStore) {
    if (cache.updateStore(oldStore, newStore) && storeChecker != null) {
      storeChecker.scheduleStoreHealthCheck(newStore);
    }
  }

  /** Clears all cache when some unexpected error occurs. */
  public void clearRegionCache() {
    cache.clearAll();
  }

  /**
   * Clears all cache when a TiKV server does not respond
   *
   * @param region region
   */
  public synchronized void onRequestFail(TiRegion region) {
    cache.invalidateRegion(region);
  }

  public void invalidateStore(long storeId) {
    cache.invalidateStore(storeId);
  }

  public void invalidateRegion(TiRegion region) {
    cache.invalidateRegion(region);
  }

  public void insertRegionToCache(TiRegion region) {
    cache.insertRegionToCache(region);
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(conf.getRawKVDefaultBackoffInMS());
  }
}
