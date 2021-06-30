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
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.util.ChannelFactory;
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
  protected long retryTimes;

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
    this.retryTimes = 0;
    if (this.targetStore.getProxyStore() != null) {
      this.timeout = conf.getForwardTimeout();
    }
  }

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
    region = newRegion;
    targetStore = regionManager.getStoreById(region.getLeader().getStoreId());
    String addressStr = targetStore.getStore().getAddress();
    ManagedChannel channel =
        channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    return true;
  }

  @Override
  public boolean onStoreUnreachable() {
    if (!conf.getEnableGrpcForward()) {
      regionManager.onRequestFail(region, targetStore);
      return false;
    }
    if (targetStore.getProxyStore() == null) {
      if (!targetStore.isUnreachable()) {
        if (checkHealth(targetStore.getStore())) {
          return true;
        }
      }
    } else if (retryTimes > region.getFollowerList().size()) {
      logger.warn(
          String.format(
              "retry time exceed for region[%d], invalid this region and store[%d]",
              region.getId(), targetStore.getId()));
      if (originStore != null) {
        regionManager.onRequestFail(region, originStore);
      }
      return false;
    }
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
    retryTimes += 1;
    logger.warn(
        String.format(
            "forward request to store [%s] by store [%s] for region[%d]",
            targetStore.getStore().getAddress(),
            targetStore.getProxyStore().getAddress(),
            region.getId()));
    String addressStr = targetStore.getProxyStore().getAddress();
    ManagedChannel channel =
        channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
    Metadata header = new Metadata();
    header.put(TiConfiguration.FORWARD_META_DATA_KEY, targetStore.getStore().getAddress());
    blockingStub = MetadataUtils.attachHeaders(TikvGrpc.newBlockingStub(channel), header);
    asyncStub = MetadataUtils.attachHeaders(TikvGrpc.newStub(channel), header);
    return true;
  }

  @Override
  protected void tryUpdateProxy() {
    if (originStore != null) {
      regionManager.updateStore(originStore, targetStore);
    }
  }

  private boolean checkHealth(Metapb.Store store) {
    String addressStr = store.getAddress();
    ManagedChannel channel =
        channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
    HealthGrpc.HealthBlockingStub stub =
        HealthGrpc.newBlockingStub(channel)
            .withDeadlineAfter(conf.getGrpcHealthCheckTimeout(), TimeUnit.MILLISECONDS);
    HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
    try {
      HealthCheckResponse resp = stub.check(req);
      if (resp.getStatus() != HealthCheckResponse.ServingStatus.SERVING) {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private TiStore switchProxyStore() {
    boolean hasVisitedStore = false;
    List<Metapb.Peer> peers = region.getFollowerList();
    for (int i = 0; i < peers.size() * 2; i++) {
      int idx = i % peers.size();
      Metapb.Peer peer = peers.get(idx);
      if (peer.getStoreId() != region.getLeader().getStoreId()) {
        if (targetStore.getProxyStore() == null) {
          TiStore store = regionManager.getStoreById(peer.getStoreId());
          if (checkHealth(store.getStore())) {
            return targetStore.withProxy(store.getStore());
          }
        } else {
          if (peer.getStoreId() == targetStore.getProxyStore().getId()) {
            hasVisitedStore = true;
          } else if (hasVisitedStore) {
            TiStore proxyStore = regionManager.getStoreById(peer.getStoreId());
            if (!proxyStore.isUnreachable() && checkHealth(proxyStore.getStore())) {
              return targetStore.withProxy(proxyStore.getStore());
            }
          }
        }
      }
    }
    return null;
  }
}
