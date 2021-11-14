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
import io.grpc.stub.MetadataUtils;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
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
    extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvFutureStub>
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
      TikvGrpc.TikvFutureStub asyncStub,
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
    } else if (!this.store.isReachable()) {
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
  protected TikvGrpc.TikvFutureStub getAsyncStub() {
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

    List<Metapb.Peer> peers = region.getFollowerList();
    if (peers.isEmpty()) {
      // no followers available, retry
      regionManager.onRequestFail(region);
      return false;
    }

    Metapb.Peer peer = switchLeader();
    if (peer == null) {
      // leader is not elected, just wait until it is ready.
      return true;
    }
    TiStore currentLeaderStore = regionManager.getStoreById(peer.getStoreId());
    if (currentLeaderStore.isReachable()) {
      // update region cache
      region = regionManager.updateLeader(region, peer.getStoreId());
      // switch to leader store
      store = currentLeaderStore;
      updateClientStub();
      return true;
    }
    if (conf.getEnableGrpcForward()) {
      // when current leader cannot be reached
      TiStore storeWithProxy = switchProxyStore();
      if (storeWithProxy == null) {
        // no store available, retry
        return false;
      }
      // use proxy store to forward requests
      regionManager.updateStore(store, storeWithProxy);
      store = storeWithProxy;
      updateClientStub();
      return true;
    }
    return false;
  }

  protected Kvrpcpb.Context makeContext(TiStoreType storeType) {
    return region.getReplicaContext(java.util.Collections.emptySet(), storeType);
  }

  protected Kvrpcpb.Context makeContext(Set<Long> resolvedLocks, TiStoreType storeType) {
    return region.getReplicaContext(resolvedLocks, storeType);
  }

  private void updateClientStub() {
    String addressStr = store.getStore().getAddress();
    if (store.getProxyStore() != null) {
      addressStr = store.getProxyStore().getAddress();
    }
    ManagedChannel channel =
        channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newFutureStub(channel);
    if (store.getProxyStore() != null) {
      Metadata header = new Metadata();
      header.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
      blockingStub = MetadataUtils.attachHeaders(blockingStub, header);
      asyncStub = MetadataUtils.attachHeaders(asyncStub, header);
    }
  }

  private Metapb.Peer switchLeader() {
    List<SwitchLeaderTask> responses = new LinkedList<>();
    for (Metapb.Peer peer : region.getFollowerList()) {
      ByteString key = region.getStartKey();
      TiStore store = regionManager.getStoreById(peer.getStoreId());
      ManagedChannel channel =
          channelFactory.getChannel(
              store.getAddress(), regionManager.getPDClient().getHostMapping());
      TikvGrpc.TikvFutureStub stub =
          TikvGrpc.newFutureStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
      Kvrpcpb.RawGetRequest rawGetRequest =
          Kvrpcpb.RawGetRequest.newBuilder()
              .setContext(region.getReplicaContext(peer))
              .setKey(key)
              .build();
      ListenableFuture<Kvrpcpb.RawGetResponse> task = stub.rawGet(rawGetRequest);
      responses.add(new SwitchLeaderTask(task, peer));
    }
    while (true) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        throw new GrpcException(e);
      }
      List<SwitchLeaderTask> unfinished = new LinkedList<>();
      for (SwitchLeaderTask task : responses) {
        if (task.task.isDone()) {
          try {
            Kvrpcpb.RawGetResponse resp = task.task.get();
            if (resp != null) {
              if (!resp.hasRegionError()) {
                // the peer is leader
                return task.peer;
              }
            }
          } catch (Exception ignored) {
          }
        } else {
          unfinished.add(task);
        }
      }
      if (unfinished.isEmpty()) {
        return null;
      }
      responses = unfinished;
    }
  }

  private TiStore switchProxyStore() {
    List<ForwardCheckTask> responses = new LinkedList<>();
    for (Metapb.Peer peer : region.getFollowerList()) {
      TiStore store = regionManager.getStoreById(peer.getStoreId());
      ManagedChannel channel =
          channelFactory.getChannel(
              store.getAddress(), regionManager.getPDClient().getHostMapping());
      HealthGrpc.HealthFutureStub stub =
          HealthGrpc.newFutureStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
      Metadata header = new Metadata();
      header.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
      HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
      ListenableFuture<HealthCheckResponse> task = stub.check(req);
      responses.add(new ForwardCheckTask(task, store));
    }
    while (true) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        throw new GrpcException(e);
      }
      List<ForwardCheckTask> unfinished = new LinkedList<>();
      for (ForwardCheckTask task : responses) {
        if (task.task.isDone()) {
          try {
            HealthCheckResponse resp = task.task.get();
            if (resp.getStatus() == HealthCheckResponse.ServingStatus.SERVING) {
              return store.withProxy(task.store.getStore());
            }
          } catch (Exception ignored) {
          }
        } else {
          unfinished.add(task);
        }
      }
      if (unfinished.isEmpty()) {
        return null;
      }
      responses = unfinished;
    }
  }

  private static class SwitchLeaderTask {
    private final ListenableFuture<Kvrpcpb.RawGetResponse> task;
    private final Metapb.Peer peer;

    private SwitchLeaderTask(ListenableFuture<Kvrpcpb.RawGetResponse> task, Metapb.Peer peer) {
      this.task = task;
      this.peer = peer;
    }
  }

  private static class ForwardCheckTask {
    private final ListenableFuture<HealthCheckResponse> task;
    private final TiStore store;

    private ForwardCheckTask(ListenableFuture<HealthCheckResponse> task, TiStore store) {
      this.task = task;
      this.store = store;
    }
  }
}
