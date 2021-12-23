/*
 *
 * Copyright 2019 TiKV Project Authors.
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
import io.grpc.stub.MetadataUtils;
import io.prometheus.client.Histogram;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;

public abstract class AbstractRegionStoreClient
    extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvFutureStub>
    implements RegionErrorReceiver {
  private static final Logger logger = LoggerFactory.getLogger(AbstractRegionStoreClient.class);

  public static final Histogram SEEK_LEADER_STORE_DURATION =
      Histogram.build()
          .name("client_java_seek_leader_store_duration")
          .help("seek leader store duration.")
          .register();

  public static final Histogram SEEK_PROXY_STORE_DURATION =
      Histogram.build()
          .name("client_java_seek_proxy_store_duration")
          .help("seek proxy store duration.")
          .register();

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
  public boolean onStoreUnreachable(BackOffer backOffer) {
    if (!store.isValid()) {
      logger.warn(String.format("store [%d] has been invalid", store.getId()));
      store = regionManager.getStoreById(store.getId(), backOffer);
      updateClientStub();
      return true;
    }

    // seek an available leader store to send request
    backOffer.checkTimeout();
    Boolean result = seekLeaderStore(backOffer);
    if (result != null) {
      return result;
    }
    if (conf.getEnableGrpcForward()) {
      // seek an available proxy store to forward request
      backOffer.checkTimeout();
      return seekProxyStore(backOffer);
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
    long deadline = timeout;
    if (store.getProxyStore() != null) {
      addressStr = store.getProxyStore().getAddress();
      deadline = conf.getForwardTimeout();
    }
    ManagedChannel channel =
        channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
    blockingStub =
        TikvGrpc.newBlockingStub(channel).withDeadlineAfter(deadline, TimeUnit.MILLISECONDS);
    asyncStub = TikvGrpc.newFutureStub(channel).withDeadlineAfter(deadline, TimeUnit.MILLISECONDS);
    if (store.getProxyStore() != null) {
      Metadata header = new Metadata();
      header.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
      blockingStub = MetadataUtils.attachHeaders(blockingStub, header);
      asyncStub = MetadataUtils.attachHeaders(asyncStub, header);
    }
  }

  private Boolean seekLeaderStore(BackOffer backOffer) {
    Histogram.Timer switchLeaderDurationTimer = SEEK_LEADER_STORE_DURATION.startTimer();
    SlowLogSpan slowLogSpan = backOffer.getSlowLog().start("seekLeaderStore");
    try {
      List<Metapb.Peer> peers = region.getFollowerList();
      if (peers.isEmpty()) {
        // no followers available, retry
        logger.warn(String.format("no followers of region[%d] available, retry", region.getId()));
        regionManager.onRequestFail(region);
        return false;
      }

      logger.info(String.format("try switch leader: region[%d]", region.getId()));

      Metapb.Peer peer = switchLeaderStore();
      if (peer != null) {
        // we found a leader
        TiStore currentLeaderStore = regionManager.getStoreById(peer.getStoreId());
        if (currentLeaderStore.isReachable()) {
          logger.info(
              String.format(
                  "update leader using switchLeader logic from store[%d] to store[%d]",
                  region.getLeader().getStoreId(), peer.getStoreId()));
          // update region cache
          TiRegion result = regionManager.updateLeader(region, peer.getStoreId());
          if (result != null) {
            region = result;
            // switch to leader store
            store = currentLeaderStore;
            updateClientStub();
          }
          return false;
        }
      } else {
        // no leader found, some response does not return normally, there may be network partition.
        logger.warn(
            String.format(
                "leader for region[%d] is not found, it is possible that network partition occurred",
                region.getId()));
      }
    } finally {
      switchLeaderDurationTimer.observeDuration();
      slowLogSpan.end();
    }
    return null;
  }

  private boolean seekProxyStore(BackOffer backOffer) {
    SlowLogSpan slowLogSpan = backOffer.getSlowLog().start("seekProxyStore");
    Histogram.Timer grpcForwardDurationTimer = SEEK_PROXY_STORE_DURATION.startTimer();
    try {
      logger.info(String.format("try grpc forward: region[%d]", region.getId()));
      // when current leader cannot be reached
      TiStore storeWithProxy = switchProxyStore();
      if (storeWithProxy == null) {
        // no store available, retry
        logger.warn(String.format("No store available, retry: region[%d]", region.getId()));
        return false;
      }
      // use proxy store to forward requests
      regionManager.updateStore(store, storeWithProxy);
      store = storeWithProxy;
      updateClientStub();
      return true;
    } finally {
      grpcForwardDurationTimer.observeDuration();
      slowLogSpan.end();
    }
  }

  // first: leader peer, second: true if any responses returned with grpc error
  private Metapb.Peer switchLeaderStore() {
    List<SwitchLeaderTask> responses = new LinkedList<>();
    for (Metapb.Peer peer : region.getFollowerList()) {
      ByteString key = region.getStartKey();
      TiStore peerStore = regionManager.getStoreById(peer.getStoreId());
      ManagedChannel channel =
          channelFactory.getChannel(
              peerStore.getAddress(), regionManager.getPDClient().getHostMapping());
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
        Thread.sleep(2);
      } catch (InterruptedException e) {
        throw new GrpcException(e);
      }
      List<SwitchLeaderTask> unfinished = new LinkedList<>();
      for (SwitchLeaderTask task : responses) {
        if (!task.task.isDone()) {
          unfinished.add(task);
          continue;
        }
        try {
          Kvrpcpb.RawGetResponse resp = task.task.get();
          if (resp != null) {
            if (!resp.hasRegionError()) {
              // the peer is leader
              logger.info(
                  String.format("rawGet response indicates peer[%d] is leader", task.peer.getId()));
              return task.peer;
            }
          }
        } catch (Exception ignored) {
        }
      }
      if (unfinished.isEmpty()) {
        return null;
      }
      responses = unfinished;
    }
  }

  private TiStore switchProxyStore() {
    long forwardTimeout = conf.getForwardTimeout();
    List<ForwardCheckTask> responses = new LinkedList<>();
    for (Metapb.Peer peer : region.getFollowerList()) {
      ByteString key = region.getStartKey();
      TiStore peerStore = regionManager.getStoreById(peer.getStoreId());
      ManagedChannel channel =
          channelFactory.getChannel(
              peerStore.getAddress(), regionManager.getPDClient().getHostMapping());
      TikvGrpc.TikvFutureStub stub =
          TikvGrpc.newFutureStub(channel).withDeadlineAfter(forwardTimeout, TimeUnit.MILLISECONDS);
      Metadata header = new Metadata();
      header.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
      Kvrpcpb.RawGetRequest rawGetRequest =
          Kvrpcpb.RawGetRequest.newBuilder()
              .setContext(region.getReplicaContext(peer))
              .setKey(key)
              .build();
      ListenableFuture<Kvrpcpb.RawGetResponse> task =
          MetadataUtils.attachHeaders(stub, header).rawGet(rawGetRequest);
      responses.add(new ForwardCheckTask(task, peerStore.getStore()));
    }
    while (true) {
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        throw new GrpcException(e);
      }
      List<ForwardCheckTask> unfinished = new LinkedList<>();
      for (ForwardCheckTask task : responses) {
        if (!task.task.isDone()) {
          unfinished.add(task);
          continue;
        }
        try {
          // any answer will do
          Kvrpcpb.RawGetResponse resp = task.task.get();
          logger.info(
              String.format(
                  "rawGetResponse indicates forward from [%s] to [%s]",
                  task.store.getAddress(), store.getAddress()));
          return store.withProxy(task.store);
        } catch (Exception ignored) {
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
    private final ListenableFuture<Kvrpcpb.RawGetResponse> task;
    private final Metapb.Store store;

    private ForwardCheckTask(ListenableFuture<Kvrpcpb.RawGetResponse> task, Metapb.Store store) {
      this.task = task;
      this.store = store;
    }
  }
}
