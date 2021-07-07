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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiClientInternalException;
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
  // TODO: the region cache logic need rewrite.
  // https://github.com/pingcap/tispark/issues/1170
  private final RegionCache cache;
  private final ReadOnlyPDClient pdClient;
  private final TiConfiguration conf;
  private final ScheduledExecutorService executor;
  private final UnreachableStoreChecker storeChecker;

  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(
      TiConfiguration conf,
      ReadOnlyPDClient pdClient,
      Function<CacheInvalidateEvent, Void> cacheInvalidateCallback) {
    this.cache = new RegionCache();
    this.pdClient = pdClient;
    this.conf = conf;
    this.cacheInvalidateCallback = cacheInvalidateCallback;
    this.executor = null;
    this.storeChecker = null;
  }

  public RegionManager(
      TiConfiguration conf,
      ReadOnlyPDClient pdClient,
      Function<CacheInvalidateEvent, Void> cacheInvalidateCallback,
      ChannelFactory channelFactory,
      boolean enableGrpcForward) {
    this.cache = new RegionCache();
    this.cacheInvalidateCallback = cacheInvalidateCallback;
    this.pdClient = pdClient;
    this.conf = conf;

    if (enableGrpcForward) {
      UnreachableStoreChecker storeChecker =
          new UnreachableStoreChecker(channelFactory, pdClient, this.cache);
      this.storeChecker = storeChecker;
      this.executor = Executors.newScheduledThreadPool(1);
      this.executor.scheduleAtFixedRate(storeChecker, 1, 1, TimeUnit.SECONDS);
    } else {
      this.storeChecker = null;
      this.executor = null;
    }
  }

  public RegionManager(TiConfiguration conf, ReadOnlyPDClient pdClient) {
    this.cache = new RegionCache();
    this.pdClient = pdClient;
    this.conf = conf;
    this.cacheInvalidateCallback = null;
    this.storeChecker = null;
    this.executor = null;
  }

  public synchronized void close() {
    if (this.executor != null) {
      this.executor.shutdownNow();
    }
  }

  public Function<CacheInvalidateEvent, Void> getCacheInvalidateCallback() {
    return cacheInvalidateCallback;
  }

  public ReadOnlyPDClient getPDClient() {
    return this.pdClient;
  }

  public TiRegion getRegionByKey(ByteString key) {
    return getRegionByKey(key, ConcreteBackOffer.newGetBackOff());
  }

  public TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
    TiRegion region = cache.getRegionByKey(key, backOffer);
    if (region == null) {
      logger.debug("Key not found in keyToRegionIdCache:" + formatBytesUTF8(key));
      Pair<Metapb.Region, Metapb.Peer> regionAndLeader = pdClient.getRegionByKey(backOffer, key);
      region =
          cache.putRegion(createRegion(regionAndLeader.first, regionAndLeader.second, backOffer));
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
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
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
    return getRegionStorePairByKey(key, storeType, ConcreteBackOffer.newGetBackOff());
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(
      ByteString key, TiStoreType storeType, BackOffer backOffer) {
    TiRegion region = getRegionByKey(key, backOffer);
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }

    TiStore store = null;
    if (storeType == TiStoreType.TiKV) {
      Peer peer = region.getCurrentReplica();
      store = getStoreById(peer.getStoreId(), backOffer);
      if (store == null) {
        cache.clearAll();
      }
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
        // clear the region cache so we may get the learner peer next time
        cache.invalidateRegion(region);
      }
    }

    if (store == null) {
      throw new TiClientInternalException(
          "Cannot find valid store on " + storeType + " for region " + region.toString());
    }

    return Pair.create(region, store);
  }

  private TiRegion createRegion(Metapb.Region region, Metapb.Peer leader, BackOffer backOffer) {
    List<Metapb.Peer> peers = region.getPeersList();
    List<TiStore> stores = getRegionStore(peers, backOffer);
    return new TiRegion(conf, region, leader, peers, stores);
  }

  private List<TiStore> getRegionStore(List<Metapb.Peer> peers, BackOffer backOffer) {
    return peers.stream().map(p -> getStoreById(p.getStoreId())).collect(Collectors.toList());
  }

  public TiStore getStoreById(long id, BackOffer backOffer) {
    try {
      TiStore store = cache.getStoreById(id);
      if (store == null) {
        store = new TiStore(pdClient.getStore(backOffer, id));
      }
      if (store.getStore().getState().equals(StoreState.Tombstone)) {
        return null;
      }
      if (cache.putStore(id, store)) {
        storeChecker.scheduleStoreHealthCheck(store);
      }
      return store;
    } catch (Exception e) {
      throw new GrpcException(e);
    }
  }

  public TiStore getStoreById(long id) {
    return getStoreById(id, ConcreteBackOffer.newGetBackOff());
  }

  public void onRegionStale(TiRegion region) {
    cache.invalidateRegion(region);
  }

  public synchronized TiRegion updateLeader(TiRegion region, long storeId) {
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
    if (cache.updateStore(oldStore, newStore)) {
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
}
