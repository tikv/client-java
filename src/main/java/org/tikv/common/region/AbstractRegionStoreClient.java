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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.operation.RegionErrorHandler;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.raw.RawKVClient;

public abstract class AbstractRegionStoreClient
    extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvStub>
    implements RegionErrorReceiver {
  private static final Logger logger = LoggerFactory.getLogger(AbstractRegionStoreClient.class);

  protected final RegionManager regionManager;
  protected TiRegion region;
  protected TiStore store;

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
    this.store = store;
    if (this.store.getProxyStore() != null) {
      this.timeout = conf.getForwardTimeout();
    } else if (!this.store.isReachable() && !this.store.canForward()) {
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

    // If we try one peer but find the leader has not changed, we do not need to try other peers.
    if (region.getLeader().getStoreId() == newRegion.getLeader().getStoreId()) {
      store = null;
    }
    region = newRegion;
    store = regionManager.getStoreById(region.getLeader().getStoreId());
    updateClientStub();
    return true;
  }

  @Override
  public boolean onStoreUnreachable() {
    if (!store.isValid()) {
      logger.warn(String.format("store [%d] has been invalid", store.getId()));
      store = regionManager.getStoreById(store.getId());
      updateClientStub();
      return true;
    }

    if (store.getProxyStore() == null) {
      if (store.isReachable()) {
        return true;
      }
    }

    CompletableFuture<Pair<Metapb.Peer, Metapb.Peer>>[] futureArray = new CompletableFuture[region.getFollowerList().size()];
    int num = 0;
    for (Metapb.Peer peer: region.getFollowerList()) {
      futureArray[num++] = CompletableFuture.supplyAsync(() -> {
        ByteString key = region.getStartKey();
        TiStore store = regionManager.getStoreById(peer.getStoreId());
        ManagedChannel channel = channelFactory.getChannel(store.getAddress(), regionManager.getPDClient().getHostMapping());
        TikvGrpc.TikvBlockingStub stub = getBlockingStub();
        Supplier<Kvrpcpb.RawGetRequest> factory =
            () -> Kvrpcpb.RawGetRequest.newBuilder().setContext(makeContext(TiStoreType.TiKV)).setKey(key).build();
        Callable<Kvrpcpb.RawGetResponse> callable = () -> ClientCalls.blockingUnaryCall(channel, TikvGrpc.getRawGetMethod(), stub.getCallOptions(), factory.get());

        try {
          Kvrpcpb.RawGetResponse resp = callable.call();
          if (resp.hasRegionError()) {
            Errorpb.Error error = resp.getRegionError();
            if (error.hasNotLeader()) {
              return Pair.create(peer, error.getNotLeader().getLeader());
            }
          }
        } catch (Exception e) {
          // ignore exception
          try {
            Thread.sleep(50);
          } catch (InterruptedException ex) {
            ex.printStackTrace();
          }
        }
        return null;
      }, Executors.newFixedThreadPool(region.getFollowerList().size()));
    }
    CompletableFuture.anyOf(futureArray).thenAccept((pair) ->{
      switchProxyStore(pair.first, pair.second);
    });

    // If this store has failed to forward request too many times, we shall try other peer at first
    // so that we can reduce the latency cost by fail requests.
    logger.warn(
        String.format(
            "retry time exceed for region[%d], invalid store[%d]",
            region.getId(), store.getId()));
    regionManager.onRequestFail(region);
    return false;
  }

  protected Kvrpcpb.Context makeContext(TiStoreType storeType) {
    return region.getReplicaContext(java.util.Collections.emptySet(), storeType);
  }

  protected Kvrpcpb.Context makeContext(Set<Long> resolvedLocks, TiStoreType storeType) {
    return region.getReplicaContext(resolvedLocks, storeType);
  }

  @Override
  public void switchLeaderOrForwardRequest() {
    if (leader != null) {
      logger.warn(
          String.format(
              "update leader to store [%d] for region[%d]",
              region.getLeader().getStoreId(), region.getId()));
      this.regionManager.updateLeader(region, leader.getStoreId());
    }
  }

  private void updateClientStub() {
    String addressStr = store.getStore().getAddress();
    if (store.getProxyStore() != null) {
      addressStr = store.getProxyStore().getAddress();
    }
    ManagedChannel channel =
        channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    if (store.getProxyStore() != null) {
      Metadata header = new Metadata();
      header.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
      blockingStub = MetadataUtils.attachHeaders(blockingStub, header);
      asyncStub = MetadataUtils.attachHeaders(asyncStub, header);
    }
  }

  private TiStore switchProxyStore(Metapb.Peer answered, Metapb.Peer leader) {
    List<Metapb.Peer>  peers = region.getFollowerList();
    if (peers.isEmpty()) {
      return null;
    }
    for(Metapb.Peer peer: peers) {
      TiStore store = regionManager.getStoreById(peer.getStoreId());
      ManagedChannel channel = channelFactory.getChannel(store.getAddress(), regionManager.getPDClient().getHostMapping());
      HealthGrpc.HealthFutureStub stub = HealthGrpc.newFutureStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
      Metadata header = new Metadata();
      header.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
      HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
      ListenableFuture<HealthCheckResponse> task = stub.check(req);
    }
  }
}
