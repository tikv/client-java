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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.grpc.ManagedChannel;
import org.tikv.common.TiConfiguration.KVMode;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.exception.GCException;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.operation.PDErrorHandler;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.FutureObserver;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.PDGrpc.PDBlockingStub;
import org.tikv.kvproto.PDGrpc.PDStub;
import org.tikv.kvproto.Pdpb.*;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.tikv.common.operation.PDErrorHandler.getRegionResponseErrorExtractor;
import static org.tikv.common.pd.PDError.buildFromPdpbError;

/**
 * PDClient is thread-safe and suggested to be shared threads
 */
public class PDClient extends AbstractGRPCClient<PDBlockingStub, PDStub>
    implements ReadOnlyPDClient {
  private RequestHeader header;
  private TsoRequest tsoReq;
  private volatile LeaderWrapper leaderWrapper;
  private ScheduledExecutorService service;
  private List<HostAndPort> pdAddrs;
  private KV etcdKV;
  private Lock lockClient;
  private Lease leaseClient;
  private static final int pdTimeOutLimit = 200;

  @Override
  public TiTimestamp getTimestamp(BackOffer backOffer) {
    Supplier<TsoRequest> request = () -> tsoReq;

    PDErrorHandler<TsoResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    TsoResponse resp = callWithRetry(backOffer, PDGrpc.METHOD_TSO, request, handler);
    Timestamp timestamp = resp.getTimestamp();
    return new TiTimestamp(timestamp.getPhysical(), timestamp.getLogical());
  }

  @Override
  public TiRegion getRegionByKey(BackOffer backOffer, ByteString key) {
    Supplier<GetRegionRequest> request;
    if (conf.getKvMode() == KVMode.RAW) {
      request = () -> GetRegionRequest.newBuilder().setHeader(header).setRegionKey(key).build();
    } else {
      CodecDataOutput cdo = new CodecDataOutput();
      BytesCodec.writeBytes(cdo, key.toByteArray());
      ByteString encodedKey = cdo.toByteString();
      request =
          () -> GetRegionRequest.newBuilder().setHeader(header).setRegionKey(encodedKey).build();
    }

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    GetRegionResponse resp = callWithRetry(backOffer, PDGrpc.METHOD_GET_REGION, request, handler);
    return new TiRegion(
        resp.getRegion(),
        resp.getLeader(),
        conf.getIsolationLevel(),
        conf.getCommandPriority(),
        conf.getKvMode());
  }

  @Override
  public Future<TiRegion> getRegionByKeyAsync(BackOffer backOffer, ByteString key) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(
            resp ->
                new TiRegion(
                    resp.getRegion(),
                    resp.getLeader(),
                    conf.getIsolationLevel(),
                    conf.getCommandPriority(),
                    conf.getKvMode()));
    Supplier<GetRegionRequest> request =
        () -> GetRegionRequest.newBuilder().setHeader(header).setRegionKey(key).build();

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    callAsyncWithRetry(backOffer, PDGrpc.METHOD_GET_REGION, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public TiRegion getRegionByID(BackOffer backOffer, long id) {
    Supplier<GetRegionByIDRequest> request =
        () -> GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    GetRegionResponse resp =
        callWithRetry(backOffer, PDGrpc.METHOD_GET_REGION_BY_ID, request, handler);
    // Instead of using default leader instance, explicitly set no leader to null
    return new TiRegion(
        resp.getRegion(),
        resp.getLeader(),
        conf.getIsolationLevel(),
        conf.getCommandPriority(),
        conf.getKvMode());
  }

  @Override
  public Future<TiRegion> getRegionByIDAsync(BackOffer backOffer, long id) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(
            resp ->
                new TiRegion(
                    resp.getRegion(),
                    resp.getLeader(),
                    conf.getIsolationLevel(),
                    conf.getCommandPriority(),
                    conf.getKvMode()));

    Supplier<GetRegionByIDRequest> request =
        () -> GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    callAsyncWithRetry(
        backOffer, PDGrpc.METHOD_GET_REGION_BY_ID, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  private Supplier<GetStoreRequest> getStoreRequest(long storeId) {
    return () -> GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
  }

  @Override
  public Store getStore(BackOffer backOffer, long storeId) {
    Supplier<GetStoreRequest> request = getStoreRequest(storeId);
    PDErrorHandler<GetStoreResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    GetStoreResponse resp = callWithRetry(backOffer, PDGrpc.METHOD_GET_STORE, request, handler);
    return resp.getStore();
  }

  @Override
  public Future<Store> getStoreAsync(BackOffer backOffer, long storeId) {
    FutureObserver<Store, GetStoreResponse> responseObserver =
        new FutureObserver<>(GetStoreResponse::getStore);

    Supplier<GetStoreRequest> request = getStoreRequest(storeId);
    PDErrorHandler<GetStoreResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    callAsyncWithRetry(backOffer, PDGrpc.METHOD_GET_STORE, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public void close() {
    if (service != null) {
      service.shutdownNow();
    }
    if (getLeaderWrapper() != null) {
      getLeaderWrapper().close();
    }
  }

  public static ReadOnlyPDClient create(TiConfiguration conf, ChannelFactory channelFactory) {
    return createRaw(conf, channelFactory);
  }

  @VisibleForTesting
  RequestHeader getHeader() {
    return header;
  }

  @VisibleForTesting
  LeaderWrapper getLeaderWrapper() {
    return leaderWrapper;
  }

  private List<URI> getMemberUrI() {
    return pdAddrs.stream().map(x -> URI.create("http://" + x.toString())).collect(Collectors.toList());
  }

  public ByteString get(ByteString key) {
    try {
      GetResponse resp = etcdKV.get(ByteSequence.from(key)).get(pdTimeOutLimit, TimeUnit.MILLISECONDS);
      List<KeyValue> kvs = resp.getKvs();
      if (kvs.size() == 0 || kvs.get(0) == null) {
        return null;
      }
      ByteSequence value = kvs.get(0).getValue();
      return ByteString.copyFrom(value.getBytes());
    } catch (Exception e) {
      throw new TiKVException("get etcd key fails", e);
    }
  }

  public ByteString get(ByteString key, ByteString defaultValue) {
    ByteString value = get(key);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public void put(ByteString key, ByteString value) {
    try {
      etcdKV.put(ByteSequence.from(key), ByteSequence.from(value)).get(pdTimeOutLimit, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new TiKVException("put etcd key fails", e);
    }
  }

  public Txn txn() {
    return etcdKV.txn();
  }

  /**
   * Tries to acquire lock with given name from PD
   *
   * @param key   name of lock
   * @param lease lease when lock expires
   * @return whether the lock is acquired successfully
   */
  public boolean lock(ByteString key, long lease) {
    try {
      LockResponse resp = lockClient.lock(ByteSequence.from(key), lease).get(pdTimeOutLimit, TimeUnit.MILLISECONDS);
      return true;
    } catch (Exception e) {
      logger.warn("lock fails", e);
      return false;
    }
  }

  /**
   * Unlock lock with given name from PD
   *
   * @param key lock name
   */
  public void unlock(ByteString key) {
    try {
      lockClient.unlock(ByteSequence.from(key)).get(pdTimeOutLimit, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      logger.warn("unlock fails", e);
    }
  }

  /**
   * Grant lease with ttl in PD
   *
   * @param ttl purposed time to live
   * @return a unique lease key
   */
  public long grantLease(long ttl) {
    try {
      LeaseGrantResponse resp = leaseClient.grant(ttl).get(pdTimeOutLimit, TimeUnit.MILLISECONDS);
      return resp.getID();
    } catch (Exception e) {
      throw new GCException("GC worker failed to start because granting lease fails", e);
    }
  }

  /**
   * Keep lease alive
   *
   * @param leaseId lease ID
   */
  public void keepLeaseAlive(long leaseId) {
    try {
      leaseClient.keepAliveOnce(leaseId).get(pdTimeOutLimit, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      logger.warn("keep alive failed", e);
    }
  }

  /**
   * Revoke lease
   *
   * @param leaseId lease ID
   */
  public void revoke(long leaseId) {
    try {
      leaseClient.revoke(leaseId).get(pdTimeOutLimit, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      logger.warn(String.format("revoke %s fails", Long.toHexString(leaseId)), e);
    }
  }

  class LeaderWrapper {
    private final String leaderInfo;
    private final PDBlockingStub blockingStub;
    private final PDStub asyncStub;
    private final long createTime;

    LeaderWrapper(
        String leaderInfo,
        PDGrpc.PDBlockingStub blockingStub,
        PDGrpc.PDStub asyncStub,
        long createTime) {
      this.leaderInfo = leaderInfo;
      this.blockingStub = blockingStub;
      this.asyncStub = asyncStub;
      this.createTime = createTime;
    }

    String getLeaderInfo() {
      return leaderInfo;
    }

    PDBlockingStub getBlockingStub() {
      return blockingStub;
    }

    PDStub getAsyncStub() {
      return asyncStub;
    }

    long getCreateTime() {
      return createTime;
    }

    void close() {
    }
  }

  public GetMembersResponse getMembers(HostAndPort url) {
    try {
      ManagedChannel probChan = channelFactory.getChannel(url.getHost() + ":" + url.getPort());
      PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(probChan);
      GetMembersRequest request =
          GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
      return stub.getMembers(request);
    } catch (Exception e) {
      logger.warn("failed to get member from pd server.", e);
    }
    return null;
  }

  private synchronized boolean switchLeader(List<String> leaderURLs) {
    if (leaderURLs.isEmpty()) return false;
    String leaderUrlStr = leaderURLs.get(0);
    // TODO: Why not strip protocol info on server side since grpc does not need it
    if (leaderWrapper != null && leaderUrlStr.equals(leaderWrapper.getLeaderInfo())) {
      return true;
    }
    // switch leader
    return createLeaderWrapper(leaderUrlStr);
  }

  private boolean createLeaderWrapper(String leaderUrlStr) {
    try {
      URL tURL = new URL(leaderUrlStr);
      HostAndPort newLeader = HostAndPort.fromParts(tURL.getHost(), tURL.getPort());
      leaderUrlStr = newLeader.toString();
      if (leaderWrapper != null && leaderUrlStr.equals(leaderWrapper.getLeaderInfo())) {
        return true;
      }

      // create new Leader
      ManagedChannel clientChannel = channelFactory.getChannel(leaderUrlStr);
      leaderWrapper =
          new LeaderWrapper(
              leaderUrlStr,
              PDGrpc.newBlockingStub(clientChannel),
              PDGrpc.newStub(clientChannel),
              System.nanoTime());
    } catch (MalformedURLException e) {
      logger.error("Error updating leader.", e);
      return false;
    }
    logger.info(String.format("Switched to new leader: %s", leaderWrapper));
    return true;
  }

  public void updateLeader() {
    for (HostAndPort url : this.pdAddrs) {
      // since resp is null, we need update leader's address by walking through all pd server.
      GetMembersResponse resp = getMembers(url);
      if (resp == null) {
        continue;
      }
      // if leader is switched, just return.
      if (switchLeader(resp.getLeader().getClientUrlsList())) {
        return;
      }
    }
    throw new TiClientInternalException(
        "already tried all address on file, but not leader found yet.");
  }

  @Override
  protected PDBlockingStub getBlockingStub() {
    if (leaderWrapper == null) {
      throw new GrpcException("PDClient may not be initialized");
    }
    return leaderWrapper
        .getBlockingStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  protected PDStub getAsyncStub() {
    if (leaderWrapper == null) {
      throw new GrpcException("PDClient may not be initialized");
    }
    return leaderWrapper
        .getAsyncStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  private PDClient(TiConfiguration conf, ChannelFactory channelFactory) {
    super(conf, channelFactory);
  }

  private void initCluster() {
    GetMembersResponse resp = null;
    List<HostAndPort> pdAddrs = getConf().getPdAddrs();
    for (HostAndPort u : pdAddrs) {
      resp = getMembers(u);
      if (resp != null) {
        break;
      }
    }
    checkNotNull(resp, "Failed to init client for PD cluster.");
    long clusterId = resp.getHeader().getClusterId();
    header = RequestHeader.newBuilder().setClusterId(clusterId).build();
    tsoReq = TsoRequest.newBuilder().setHeader(header).setCount(1).build();
    this.pdAddrs = pdAddrs;
    createLeaderWrapper(resp.getLeader().getClientUrls(0));
    service =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build());
    service.scheduleAtFixedRate(
        () -> {
          // Wrap this with a try catch block in case schedule update fails
          try {
            updateLeader();
          } catch (Exception e) {
            logger.warn("Update leader failed", e);
          }
        },
        1,
        1,
        TimeUnit.MINUTES);
    Client client = Client.builder().endpoints(getMemberUrI()).build();
    etcdKV = client.getKVClient();
    lockClient = client.getLockClient();
    leaseClient = client.getLeaseClient();
  }

  static PDClient createRaw(TiConfiguration conf, ChannelFactory channelFactory) {
    PDClient client = null;
    try {
      client = new PDClient(conf, channelFactory);
      client.initCluster();
    } catch (Exception e) {
      if (client != null) {
        try {
          client.close();
        } catch (TiKVException ignore) {
        }
      }
      throw e;
    }
    return client;
  }
}
