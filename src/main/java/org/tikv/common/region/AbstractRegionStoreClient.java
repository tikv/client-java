/*
 *
 * Copyright 2019 PingCAP, Inc.
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.util.ChannelFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;

public abstract class AbstractRegionStoreClient
    extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvStub>
    implements RegionErrorReceiver {
  private static final Logger logger = LoggerFactory.getLogger(AbstractRegionStoreClient.class);

  protected final RegionManager regionManager;
  protected TiRegion region;
  protected TiStore targetStore;
  protected TiStore originStore;
  private long retryForwardTimes;
  private long retryLeaderTimes;
  private Metapb.Peer candidateLeader;

  protected AbstractRegionStoreClient(
      TiConfiguration conf,
      TiRegion region,
      TiStore store,
      ChannelFactory channelFactory,
      TikvGrpc.TikvBlockingStub blockingStub,
      TikvGrpc.TikvStub asyncStub,
      RegionManager regionManager) {
    super(conf, channelFactory, blockingStub, asyncStub);
    checkNotNull(region, "Region is empty");
    checkNotNull(region.getLeader(), "Leader Peer is null");
    checkArgument(region.getLeader() != null, "Leader Peer is null");
    this.region = region;
    this.regionManager = regionManager;
    this.targetStore = store;
    this.originStore = null;
    this.candidateLeader = null;
    this.retryForwardTimes = 0;
    this.retryLeaderTimes = 0;
    if (this.targetStore.getProxyStore() != null) {
      this.timeout = conf.getForwardTimeout();
    } else if (!this.targetStore.isReachable() && !this.targetStore.canForwardFirst()) {
      onStoreUnreachable();
    }
  }

  @Override
  public TiRegion getRegion() {
    return region;
  }

  @Override
  protected TikvGrpc.TikvBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected TikvGrpc.TikvStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws GrpcException {}

  /**
   * onNotLeader deals with NotLeaderError and returns whether re-splitting key range is needed
   *
   * @param newRegion the new region presented by NotLeader Error
   * @return false when re-split is needed.
   */
  @Override
  public boolean onNotLeader(TiRegion newRegion) {
    if (logger.isDebugEnabled()) {
      logger.debug(region + ", new leader = " + newRegion.getLeader().getStoreId());
    }
    // When switch leader fails or the region changed its region epoch,
    // it would be necessary to re-split task's key range for new region.
    if (!region.getRegionEpoch().equals(newRegion.getRegionEpoch())) {
      return false;
    }

    // If we try one peer but find the leader has not changed, we do not need try other peers.
    if (candidateLeader != null
        && region.getLeader().getStoreId() == newRegion.getLeader().getStoreId()) {
      retryLeaderTimes = newRegion.getFollowerList().size();
      originStore = null;
    }
    candidateLeader = null;
    region = newRegion;
    targetStore = regionManager.getStoreById(region.getLeader().getStoreId());
    updateClientStub();
    return true;
  }

  @Override
  public boolean onStoreUnreachable() {
    if (!targetStore.isValid()) {
      logger.warn(
          String.format("store [%d] has been invalid", region.getId(), targetStore.getId()));
      targetStore = regionManager.getStoreById(targetStore.getId());
      updateClientStub();
      return true;
    }

    if (targetStore.getProxyStore() == null) {
      if (targetStore.isReachable()) {
        return true;
      }
    }

    // If this store has failed to forward request too many times, we shall try other peer at first
    // so that we can
    // reduce the latency cost by fail requests.
    if (targetStore.canForwardFirst()) {
      if (conf.getEnableGrpcForward() && retryForwardTimes <= region.getFollowerList().size()) {
        return retryOtherStoreByProxyForward();
      }
      if (retryOtherStoreLeader()) {
        return true;
      }
    } else {
      if (retryOtherStoreLeader()) {
        return true;
      }
      if (conf.getEnableGrpcForward() && retryForwardTimes <= region.getFollowerList().size()) {
        return retryOtherStoreByProxyForward();
      }
      return true;
    }

    logger.warn(
        String.format(
            "retry time exceed for region[%d], invalid this region[%d]",
            region.getId(), targetStore.getId()));
    regionManager.onRequestFail(region);
    return false;
  }

  protected Kvrpcpb.Context makeContext(TiStoreType storeType) {
    if (candidateLeader != null && storeType == TiStoreType.TiKV) {
      return region.getReplicaContext(candidateLeader, java.util.Collections.emptySet());
    } else {
      return region.getReplicaContext(java.util.Collections.emptySet(), storeType);
    }
  }

  protected Kvrpcpb.Context makeContext(Set<Long> resolvedLocks, TiStoreType storeType) {
    if (candidateLeader != null && storeType == TiStoreType.TiKV) {
      return region.getReplicaContext(candidateLeader, resolvedLocks);
    } else {
      return region.getReplicaContext(resolvedLocks, storeType);
    }
  }

  @Override
  public void tryUpdateRegionStore() {
    if (originStore != null) {
      if (originStore.getId() == targetStore.getId()) {
        logger.warn(
            String.format(
                "update store [%s] by proxy-store [%s]",
                targetStore.getStore().getAddress(), targetStore.getProxyStore().getAddress()));
        // We do not need to mark the store can-forward, because if one store has grpc forward
        // successfully, it will
        // create a new store object, which is can-forward.
        regionManager.updateStore(originStore, targetStore);
      } else {
        // If we try to forward request to leader by follower failed, it means that the store of old
        // leader may be
        // unavailable but the new leader has not been report to PD. So we can ban this store for a
        // short time to
        // avoid too many request try forward rather than try other peer.
        originStore.forwardFail();
      }
    }
    if (candidateLeader != null) {
      logger.warn(
          String.format(
              "update leader to store [%d] for region[%d]",
              candidateLeader.getStoreId(), region.getId()));
      this.regionManager.updateLeader(region, candidateLeader.getStoreId());
    }
  }

  private boolean retryOtherStoreLeader() {
    List<Metapb.Peer> peers = region.getFollowerList();
    if (retryLeaderTimes >= peers.size()) {
      return false;
    }
    retryLeaderTimes += 1;
    boolean hasVisitedStore = false;
    for (Metapb.Peer cur : peers) {
      if (candidateLeader == null || hasVisitedStore) {
        TiStore store = regionManager.getStoreById(cur.getStoreId());
        if (store != null && store.isReachable()) {
          targetStore = store;
          candidateLeader = cur;
          logger.warn(
              String.format(
                  "try store [%d],peer[%d] for region[%d], which may be new leader",
                  targetStore.getId(), candidateLeader.getId(), region.getId()));
          updateClientStub();
          return true;
        } else {
          continue;
        }
      } else if (candidateLeader.getId() == cur.getId()) {
        hasVisitedStore = true;
      }
    }
    candidateLeader = null;
    retryLeaderTimes = peers.size();
    return false;
  }

  private void updateClientStub() {
    String addressStr = targetStore.getStore().getAddress();
    if (targetStore.getProxyStore() != null) {
      addressStr = targetStore.getProxyStore().getAddress();
    }
    ManagedChannel channel =
        channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    if (targetStore.getProxyStore() != null) {
      Metadata header = new Metadata();
      header.put(TiConfiguration.FORWARD_META_DATA_KEY, targetStore.getStore().getAddress());
      blockingStub = MetadataUtils.attachHeaders(blockingStub, header);
      asyncStub = MetadataUtils.attachHeaders(asyncStub, header);
    }
  }

  private boolean retryOtherStoreByProxyForward() {
    TiStore proxyStore = switchProxyStore();
    if (proxyStore == null) {
      logger.warn(
          String.format(
              "no forward store can be selected for store [%s] and region[%d]",
              targetStore.getStore().getAddress(), region.getId()));
      return false;
    }
    if (originStore == null) {
      originStore = targetStore;
      if (this.targetStore.getProxyStore() != null) {
        this.timeout = conf.getForwardTimeout();
      }
    }
    targetStore = proxyStore;
    retryForwardTimes += 1;
    updateClientStub();
    logger.warn(
        String.format(
            "forward request to store [%s] by store [%s] for region[%d]",
            targetStore.getStore().getAddress(),
            targetStore.getProxyStore().getAddress(),
            region.getId()));
    return true;
  }

  private TiStore switchProxyStore() {
    boolean hasVisitedStore = false;
    List<Metapb.Peer> peers = region.getFollowerList();
    if (peers.isEmpty()) {
      return null;
    }
    Metapb.Store proxyStore = targetStore.getProxyStore();
    if (proxyStore == null || peers.get(peers.size() - 1).getStoreId() == proxyStore.getId()) {
      hasVisitedStore = true;
    }
    for (Metapb.Peer peer : peers) {
      if (hasVisitedStore) {
        TiStore store = regionManager.getStoreById(peer.getStoreId());
        if (store.isReachable()) {
          return targetStore.withProxy(store.getStore());
        }
      } else if (peer.getStoreId() == proxyStore.getId()) {
        hasVisitedStore = true;
      }
    }
    return null;
  }
}
