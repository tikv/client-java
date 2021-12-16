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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.tikv.common.operation.PDErrorHandler.getRegionResponseErrorExtractor;
import static org.tikv.common.pd.PDError.buildFromPdpbError;
import static org.tikv.common.pd.PDUtils.addrToUri;
import static org.tikv.common.pd.PDUtils.uriToAddr;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.prometheus.client.Histogram;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration.KVMode;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.log.SlowLog;
import org.tikv.common.log.SlowLogEmptyImpl;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.operation.NoopHandler;
import org.tikv.common.operation.PDErrorHandler;
import org.tikv.common.util.BackOffFunction.BackOffFuncType;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.*;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.PDGrpc.PDBlockingStub;
import org.tikv.kvproto.PDGrpc.PDFutureStub;
import org.tikv.kvproto.Pdpb.Error;
import org.tikv.kvproto.Pdpb.ErrorType;
import org.tikv.kvproto.Pdpb.GetAllStoresRequest;
import org.tikv.kvproto.Pdpb.GetMembersRequest;
import org.tikv.kvproto.Pdpb.GetMembersResponse;
import org.tikv.kvproto.Pdpb.GetOperatorRequest;
import org.tikv.kvproto.Pdpb.GetOperatorResponse;
import org.tikv.kvproto.Pdpb.GetRegionByIDRequest;
import org.tikv.kvproto.Pdpb.GetRegionRequest;
import org.tikv.kvproto.Pdpb.GetRegionResponse;
import org.tikv.kvproto.Pdpb.GetStoreRequest;
import org.tikv.kvproto.Pdpb.GetStoreResponse;
import org.tikv.kvproto.Pdpb.OperatorStatus;
import org.tikv.kvproto.Pdpb.RequestHeader;
import org.tikv.kvproto.Pdpb.ResponseHeader;
import org.tikv.kvproto.Pdpb.ScatterRegionRequest;
import org.tikv.kvproto.Pdpb.ScatterRegionResponse;
import org.tikv.kvproto.Pdpb.Timestamp;
import org.tikv.kvproto.Pdpb.TsoRequest;
import org.tikv.kvproto.Pdpb.TsoResponse;

public class PDClient extends AbstractGRPCClient<PDBlockingStub, PDFutureStub>
    implements ReadOnlyPDClient {
  private static final String TIFLASH_TABLE_SYNC_PROGRESS_PATH = "/tiflash/table/sync";
  private static final long MIN_TRY_UPDATE_DURATION = 50;
  private final Logger logger = LoggerFactory.getLogger(PDClient.class);
  private RequestHeader header;
  private TsoRequest tsoReq;
  private volatile PDClientWrapper pdClientWrapper;
  private ScheduledExecutorService service;
  private ScheduledExecutorService tiflashReplicaService;
  private List<URI> pdAddrs;
  private Client etcdClient;
  private ConcurrentMap<Long, Double> tiflashReplicaMap;
  private HostMapping hostMapping;
  private final AtomicLong lastUpdateLeaderTime = new AtomicLong(0);

  public static final Histogram PD_GET_REGION_BY_KEY_REQUEST_LATENCY =
      Histogram.build()
          .name("client_java_pd_get_region_by_requests_latency")
          .help("pd getRegionByKey request latency.")
          .register();

  public static final Histogram PD_SEEK_LEADER_DURATION =
      Histogram.build()
          .name("client_java_pd_seek_leader_duration")
          .help("pd seek leader duration.")
          .register();

  public static final Histogram PD_SEEK_PROXY_DURATION =
      Histogram.build()
          .name("client_java_pd_seek_proxy_duration")
          .help("pd seek proxy duration.")
          .register();

  private PDClient(TiConfiguration conf, ChannelFactory channelFactory) {
    super(conf, channelFactory);
    initCluster();
    this.blockingStub = getBlockingStub();
    this.asyncStub = getAsyncStub();
  }

  public static ReadOnlyPDClient create(TiConfiguration conf, ChannelFactory channelFactory) {
    return createRaw(conf, channelFactory);
  }

  static PDClient createRaw(TiConfiguration conf, ChannelFactory channelFactory) {
    return new PDClient(conf, channelFactory);
  }

  public HostMapping getHostMapping() {
    return hostMapping;
  }

  @Override
  public TiTimestamp getTimestamp(BackOffer backOffer) {
    Supplier<TsoRequest> request = () -> tsoReq;

    PDErrorHandler<TsoResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    TsoResponse resp = callWithRetry(backOffer, PDGrpc.getTsoMethod(), request, handler);
    Timestamp timestamp = resp.getTimestamp();
    return new TiTimestamp(timestamp.getPhysical(), timestamp.getLogical());
  }

  /**
   * Sends request to pd to scatter region.
   *
   * @param region represents a region info
   */
  void scatterRegion(Metapb.Region region, BackOffer backOffer) {
    Supplier<ScatterRegionRequest> request =
        () ->
            ScatterRegionRequest.newBuilder().setHeader(header).setRegionId(region.getId()).build();

    PDErrorHandler<ScatterRegionResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    ScatterRegionResponse resp =
        callWithRetry(backOffer, PDGrpc.getScatterRegionMethod(), request, handler);
    // TODO: maybe we should retry here, need dig into pd's codebase.
    if (resp.hasHeader() && resp.getHeader().hasError()) {
      throw new TiClientInternalException(
          String.format("failed to scatter region because %s", resp.getHeader().getError()));
    }
  }

  /**
   * wait scatter region until finish
   *
   * @param region
   */
  void waitScatterRegionFinish(Metapb.Region region, BackOffer backOffer) {
    for (; ; ) {
      GetOperatorResponse resp = getOperator(region.getId());
      if (resp != null) {
        if (isScatterRegionFinish(resp)) {
          logger.info(String.format("wait scatter region on %d is finished", region.getId()));
          return;
        } else {
          backOffer.doBackOff(
              BackOffFuncType.BoRegionMiss, new GrpcException("waiting scatter region"));
          logger.info(
              String.format(
                  "wait scatter region %d at key %s is %s",
                  region.getId(),
                  KeyUtils.formatBytes(resp.getDesc().toByteArray()),
                  resp.getStatus().toString()));
        }
      }
    }
  }

  private GetOperatorResponse getOperator(long regionId) {
    Supplier<GetOperatorRequest> request =
        () -> GetOperatorRequest.newBuilder().setHeader(header).setRegionId(regionId).build();
    // get operator no need to handle error and no need back offer.
    return callWithRetry(
        ConcreteBackOffer.newCustomBackOff(0),
        PDGrpc.getGetOperatorMethod(),
        request,
        new NoopHandler<>());
  }

  private boolean isScatterRegionFinish(GetOperatorResponse resp) {
    // If the current operator of region is not `scatter-region`, we could assume
    // that `scatter-operator` has finished or timeout.
    boolean finished =
        !resp.getDesc().equals(ByteString.copyFromUtf8("scatter-region"))
            || resp.getStatus() != OperatorStatus.RUNNING;

    if (resp.hasHeader()) {
      ResponseHeader header = resp.getHeader();
      if (header.hasError()) {
        Error error = header.getError();
        // heartbeat may not send to PD
        if (error.getType() == ErrorType.REGION_NOT_FOUND) {
          finished = true;
        }
      }
    }
    return finished;
  }

  @Override
  public Pair<Metapb.Region, Metapb.Peer> getRegionByKey(BackOffer backOffer, ByteString key) {
    Histogram.Timer requestTimer = PD_GET_REGION_BY_KEY_REQUEST_LATENCY.startTimer();
    try {
      if (conf.getKvMode() == KVMode.TXN) {
        CodecDataOutput cdo = new CodecDataOutput();
        BytesCodec.writeBytes(cdo, key.toByteArray());
        key = cdo.toByteString();
      }
      ByteString queryKey = key;

      Supplier<GetRegionRequest> request =
          () -> GetRegionRequest.newBuilder().setHeader(header).setRegionKey(queryKey).build();

      PDErrorHandler<GetRegionResponse> handler =
          new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

      while (true) {
        try {
          GetRegionResponse resp =
              callWithRetry(backOffer, PDGrpc.getGetRegionMethod(), request, handler);
          return new Pair<Metapb.Region, Metapb.Peer>(
              decodeRegion(resp.getRegion()), resp.getLeader());
        } catch (Exception e) {
          backOffer.doBackOff(BackOffFuncType.BoPDRPC, e);
        }
      }
    } finally {
      requestTimer.observeDuration();
    }
  }

  @Override
  public Pair<Metapb.Region, Metapb.Peer> getRegionByID(BackOffer backOffer, long id) {
    Supplier<GetRegionByIDRequest> request =
        () -> GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    GetRegionResponse resp =
        callWithRetry(backOffer, PDGrpc.getGetRegionByIDMethod(), request, handler);
    return new Pair<Metapb.Region, Metapb.Peer>(decodeRegion(resp.getRegion()), resp.getLeader());
  }

  private Supplier<GetStoreRequest> buildGetStoreReq(long storeId) {
    return () -> GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
  }

  private Supplier<GetAllStoresRequest> buildGetAllStoresReq() {
    return () -> GetAllStoresRequest.newBuilder().setHeader(header).build();
  }

  private <T> PDErrorHandler<GetStoreResponse> buildPDErrorHandler() {
    return new PDErrorHandler<>(
        r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null, this);
  }

  @Override
  public Store getStore(BackOffer backOffer, long storeId) {
    GetStoreResponse resp =
        callWithRetry(
            backOffer,
            PDGrpc.getGetStoreMethod(),
            buildGetStoreReq(storeId),
            buildPDErrorHandler());
    if (resp != null) {
      return resp.getStore();
    }
    return null;
  }

  @Override
  public List<Store> getAllStores(BackOffer backOffer) {
    return callWithRetry(
            backOffer,
            PDGrpc.getGetAllStoresMethod(),
            buildGetAllStoresReq(),
            new PDErrorHandler<>(
                r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
                this))
        .getStoresList();
  }

  @Override
  public TiConfiguration.ReplicaRead getReplicaRead() {
    return conf.getReplicaRead();
  }

  @Override
  public void close() throws InterruptedException {
    etcdClient.close();
    if (service != null) {
      service.shutdownNow();
    }
    if (tiflashReplicaService != null) {
      tiflashReplicaService.shutdownNow();
    }
    if (channelFactory != null) {
      channelFactory.close();
    }
  }

  @VisibleForTesting
  RequestHeader getHeader() {
    return header;
  }

  @VisibleForTesting
  PDClientWrapper getPdClientWrapper() {
    return pdClientWrapper;
  }

  private GetMembersResponse getMembers(URI uri) {
    try {
      ManagedChannel probChan = channelFactory.getChannel(uriToAddr(uri), hostMapping);
      PDGrpc.PDBlockingStub stub =
          PDGrpc.newBlockingStub(probChan).withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
      GetMembersRequest request =
          GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
      GetMembersResponse resp = stub.getMembers(request);
      // check if the response contains a valid leader
      if (resp != null && resp.getLeader().getMemberId() == 0) {
        return null;
      }
      return resp;
    } catch (Exception e) {
      logger.warn("failed to get member from pd server.", e);
    }
    return null;
  }

  private ListenableFuture<GetMembersResponse> getMembersAsync(URI uri) {
    ManagedChannel probChan = channelFactory.getChannel(uriToAddr(uri), hostMapping);
    PDGrpc.PDFutureStub stub =
        PDGrpc.newFutureStub(probChan).withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
    GetMembersRequest request =
        GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
    return stub.getMembers(request);
  }

  private ListenableFuture<GetMembersResponse> getMembersForwardAsync(URI proxy, URI leader) {
    ManagedChannel probChan = channelFactory.getChannel(uriToAddr(proxy), hostMapping);
    PDGrpc.PDFutureStub stub =
        PDGrpc.newFutureStub(probChan)
            .withDeadlineAfter(conf.getForwardTimeout(), TimeUnit.MILLISECONDS);
    Metadata header = new Metadata();
    header.put(TiConfiguration.PD_FORWARD_META_DATA_KEY, uriToAddr(leader));
    GetMembersRequest request =
        GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
    return stub.getMembers(request);
  }

  // return whether the leader has changed to target address `leaderUrlStr`.
  synchronized boolean trySwitchLeader(String leaderUrlStr) {
    if (pdClientWrapper != null) {
      if (leaderUrlStr.equals(pdClientWrapper.getLeaderAddr())) {
        // The message to leader is not forwarded by follower.
        if (leaderUrlStr.equals(pdClientWrapper.getProxyAddr())) {
          return true;
        }
      }
      // If leader has transferred to another member, we can create another leader wrapper.
    }
    // switch leader
    return createLeaderClientWrapper(leaderUrlStr);
  }

  private synchronized boolean createLeaderClientWrapper(String leaderUrlStr) {
    try {
      // create new Leader
      ManagedChannel clientChannel = channelFactory.getChannel(leaderUrlStr, hostMapping);
      pdClientWrapper =
          new PDClientWrapper(
              leaderUrlStr, leaderUrlStr, getTimeout(), clientChannel, System.nanoTime());
      timeout = conf.getTimeout();
    } catch (IllegalArgumentException e) {
      return false;
    }
    logger.info(String.format("Switched to new leader: %s", pdClientWrapper));
    return true;
  }

  synchronized boolean createFollowerClientWrapper(String followerUrlStr, String leaderUrls) {
    // TODO: Why not strip protocol info on server side since grpc does not need it

    try {
      // create new Leader
      ManagedChannel channel = channelFactory.getChannel(followerUrlStr, hostMapping);
      timeout = conf.getForwardTimeout();
      pdClientWrapper =
          new PDClientWrapper(leaderUrls, followerUrlStr, getTimeout(), channel, System.nanoTime());
    } catch (IllegalArgumentException e) {
      return false;
    }
    logger.info(String.format("Switched to new leader by follower forward: %s", pdClientWrapper));
    return true;
  }

  public boolean updateLeaderOrForwardFollower(SlowLog slowLog) {
    if (System.currentTimeMillis() - lastUpdateLeaderTime.get() < MIN_TRY_UPDATE_DURATION) {
      return false;
    }

    Histogram.Timer seekLeaderDurationTimer = PD_SEEK_LEADER_DURATION.startTimer();
    SlowLogSpan span1 = slowLog.start("PD Seek Leader");
    Pdpb.Member leader;
    try {
      leader = seekLeaderPD();
      if (leader == null) {
        // no leader found, retry
        logger.warn("failed to fetch leader for pd");
        return false;
      } else {
        // we found a leader
        String leaderUrlStr = leader.getClientUrlsList().get(0);
        leaderUrlStr = uriToAddr(addrToUri(leaderUrlStr));
        if (checkHealth(leaderUrlStr, hostMapping) && trySwitchLeader(leaderUrlStr)) {
          // if leader is switched, just return.
          lastUpdateLeaderTime.set(System.currentTimeMillis());
          return true;
        }
      }
    } finally {
      seekLeaderDurationTimer.observeDuration();
      span1.end();
    }
    // the leader is unreachable, update leader failed
    if (conf.getEnableGrpcForward()) {
      Histogram.Timer seekProxyDurationTimer = PD_SEEK_PROXY_DURATION.startTimer();
      SlowLogSpan span2 = slowLog.start("PD Seek Proxy");
      try {
        return seekProxyPD(leader);
      } finally {
        seekProxyDurationTimer.observeDuration();
        span2.end();
      }
    }
    return false;
  }

  private Pdpb.Member seekLeaderPD() {
    List<SeekLeaderPDTask> responses = new LinkedList<>();
    List<URI> addrs = new ArrayList<>(this.pdAddrs);
    for (URI url : addrs) {
      ListenableFuture<GetMembersResponse> task = getMembersAsync(url);
      responses.add(new SeekLeaderPDTask(task, uriToAddr(url)));
    }
    while (true) {
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        throw new GrpcException(e);
      }
      List<SeekLeaderPDTask> unfinished = new LinkedList<>();
      for (SeekLeaderPDTask task : responses) {
        if (!task.task.isDone()) {
          unfinished.add(task);
          continue;
        }
        try {
          GetMembersResponse resp = task.task.get();
          if (resp != null) {
            if (!resp.getLeader().getClientUrlsList().isEmpty()) {
              // the leader exists
              logger.info(String.format("getMembers indicate [%s] is pd leader", task.addr));

              List<Pdpb.Member> members = resp.getMembersList();
              tryUpdateMembers(
                  addrs,
                  members
                      .stream()
                      .map(member -> addrToUri(member.getClientUrls(0)))
                      .collect(Collectors.toList()));
              for (SeekLeaderPDTask unfinishedTasks : responses) {
                if (!unfinishedTasks.task.isDone()) {
                  unfinishedTasks.task.cancel(true);
                }
              }
              return resp.getLeader();
            }
          }
        } catch (Exception ignored) {
        }
      }
      if (unfinished.isEmpty()) {
        break;
      }
      responses = unfinished;
    }
    return null;
  }

  private boolean seekProxyPD(Pdpb.Member leader) {
    String leaderUrlStr = leader.getClientUrlsList().get(0);

    List<SeekProxyPDTask> responses = new ArrayList<>();
    List<URI> addrs = new ArrayList<>(this.pdAddrs);
    for (URI url : addrs) {
      String followerUrlStr = uriToAddr(url);
      if (followerUrlStr.equals(leader.getClientUrlsList().get(0))) {
        continue;
      }
      ListenableFuture<GetMembersResponse> task =
          getMembersForwardAsync(url, addrToUri(leaderUrlStr));
      responses.add(new SeekProxyPDTask(task, followerUrlStr));
    }

    while (true) {
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        throw new GrpcException(e);
      }
      List<SeekProxyPDTask> unfinished = new LinkedList<>();
      for (SeekProxyPDTask task : responses) {
        if (!task.task.isDone()) {
          unfinished.add(task);
          continue;
        }
        try {
          // any answer will do
          GetMembersResponse resp = task.task.get();
          if (resp != null) {
            logger.info(
                String.format(
                    "getMembersResponse indicates forward from [%s] to [%s]",
                    task.addr, leaderUrlStr));
            for (SeekProxyPDTask unfinishedTasks : responses) {
              if (!unfinishedTasks.task.isDone()) {
                unfinishedTasks.task.cancel(true);
              }
            }
            return createFollowerClientWrapper(task.addr, leaderUrlStr);
          }
        } catch (Exception ignored) {
        }
      }
      if (unfinished.isEmpty()) {
        break;
      }
      responses = unfinished;
    }
    // already tried all members, none of the members is reachable
    return false;
  }

  private synchronized void tryUpdateMembers(List<URI> originalMembers, List<URI> members) {
    if (originalMembers.equals(members)) {
      this.pdAddrs = members;
    }
  }

  public void updateTiFlashReplicaStatus() {
    ByteSequence prefix =
        ByteSequence.from(TIFLASH_TABLE_SYNC_PROGRESS_PATH, StandardCharsets.UTF_8);
    for (int i = 0; i < 5; i++) {
      CompletableFuture<GetResponse> resp;
      try {
        resp =
            etcdClient.getKVClient().get(prefix, GetOption.newBuilder().withPrefix(prefix).build());
      } catch (Exception e) {
        logger.info("get tiflash table replica sync progress failed, continue checking.", e);
        continue;
      }
      GetResponse getResp;
      try {
        getResp = resp.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        continue;
      } catch (ExecutionException e) {
        throw new GrpcException("failed to update tiflash replica", e);
      }
      ConcurrentMap<Long, Double> progressMap = new ConcurrentHashMap<>();
      for (KeyValue kv : getResp.getKvs()) {
        long tableId;
        try {
          tableId =
              Long.parseLong(
                  kv.getKey().toString().substring(TIFLASH_TABLE_SYNC_PROGRESS_PATH.length()));
        } catch (Exception e) {
          logger.info(
              "invalid tiflash table replica sync progress key. key = " + kv.getKey().toString());
          continue;
        }
        double progress;
        try {
          progress = Double.parseDouble(kv.getValue().toString());
        } catch (Exception e) {
          logger.info(
              "invalid tiflash table replica sync progress value. value = "
                  + kv.getValue().toString());
          continue;
        }
        progressMap.put(tableId, progress);
      }
      tiflashReplicaMap = progressMap;
      break;
    }
  }

  public double getTiFlashReplicaProgress(long tableId) {
    return tiflashReplicaMap.getOrDefault(tableId, 0.0);
  }

  @Override
  protected PDBlockingStub getBlockingStub() {
    if (pdClientWrapper == null) {
      throw new GrpcException("PDClient may not be initialized");
    }
    return pdClientWrapper.getBlockingStub();
  }

  @Override
  protected PDFutureStub getAsyncStub() {
    if (pdClientWrapper == null) {
      throw new GrpcException("PDClient may not be initialized");
    }
    return pdClientWrapper.getAsyncStub();
  }

  private void initCluster() {
    logger.info("init cluster: start");
    GetMembersResponse resp = null;
    List<URI> pdAddrs = new ArrayList<>(getConf().getPdAddrs());
    // shuffle PD addresses so that clients call getMembers from different PD
    Collections.shuffle(pdAddrs);
    this.pdAddrs = pdAddrs;
    this.etcdClient =
        Client.builder()
            .endpoints(pdAddrs)
            .executorService(
                Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder()
                        .setNameFormat("etcd-conn-manager-pool-%d")
                        .setDaemon(true)
                        .build()))
            .build();
    logger.info("init host mapping: start");
    this.hostMapping =
        Optional.ofNullable(getConf().getHostMapping())
            .orElseGet(() -> new DefaultHostMapping(this.etcdClient, conf.getNetworkMappingName()));
    logger.info("init host mapping: end");
    // The first request may cost too much latency
    long originTimeout = this.timeout;
    this.timeout = 2000;
    for (URI u : pdAddrs) {
      logger.info("get members with pd " + u + ": start");
      resp = getMembers(u);
      logger.info("get members with pd " + u + ": end");
      if (resp != null) {
        break;
      }
    }
    if (resp == null) {
      logger.error("Could not get leader member with: " + pdAddrs);
    }

    this.timeout = originTimeout;
    checkNotNull(resp, "Failed to init client for PD cluster.");
    long clusterId = resp.getHeader().getClusterId();
    header = RequestHeader.newBuilder().setClusterId(clusterId).build();
    tsoReq = TsoRequest.newBuilder().setHeader(header).setCount(1).build();
    this.tiflashReplicaMap = new ConcurrentHashMap<>();
    this.pdAddrs =
        resp.getMembersList()
            .stream()
            .map(mem -> addrToUri(mem.getClientUrls(0)))
            .collect(Collectors.toList());
    logger.info("init cluster with address: " + this.pdAddrs);

    String leaderUrlStr = resp.getLeader().getClientUrls(0);
    leaderUrlStr = uriToAddr(addrToUri(leaderUrlStr));
    logger.info("createLeaderClientWrapper with leader " + leaderUrlStr + ": start");
    createLeaderClientWrapper(leaderUrlStr);
    logger.info("createLeaderClientWrapper with leader " + leaderUrlStr + ": end");
    service =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("PDClient-update-leader-pool-%d")
                .setDaemon(true)
                .build());
    service.scheduleAtFixedRate(
        () -> {
          // Wrap this with a try catch block in case schedule update fails
          try {
            updateLeaderOrForwardFollower(SlowLogEmptyImpl.INSTANCE);
          } catch (Exception e) {
            logger.warn("Update leader failed", e);
          }
        },
        10,
        10,
        TimeUnit.SECONDS);
    tiflashReplicaService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("PDClient-tiflash-replica-pool-%d")
                .setDaemon(true)
                .build());
    tiflashReplicaService.scheduleAtFixedRate(
        this::updateTiFlashReplicaStatus, 10, 10, TimeUnit.SECONDS);
    logger.info("init cluster: finish");
  }

  static class PDClientWrapper {
    private final String leaderAddr;
    private final PDBlockingStub blockingStub;
    private final PDFutureStub asyncStub;
    private final long createTime;
    private final String proxyAddr;

    PDClientWrapper(
        String leaderAddr,
        String proxyAddr,
        long timeout,
        ManagedChannel clientChannel,
        long createTime) {
      timeout *= 100;
      if (!proxyAddr.equals(leaderAddr)) {
        Metadata header = new Metadata();
        header.put(TiConfiguration.PD_FORWARD_META_DATA_KEY, addrToUri(leaderAddr).toString());
        this.blockingStub =
            MetadataUtils.attachHeaders(
                PDGrpc.newBlockingStub(clientChannel)
                    .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS),
                header);
        this.asyncStub =
            MetadataUtils.attachHeaders(
                PDGrpc.newFutureStub(clientChannel)
                    .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS),
                header);
      } else {
        this.blockingStub =
            PDGrpc.newBlockingStub(clientChannel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
        this.asyncStub =
            PDGrpc.newFutureStub(clientChannel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
      }
      this.leaderAddr = leaderAddr;
      this.proxyAddr = proxyAddr;
      this.createTime = createTime;
    }

    String getLeaderAddr() {
      return leaderAddr;
    }

    String getProxyAddr() {
      return proxyAddr;
    }

    PDBlockingStub getBlockingStub() {
      return blockingStub;
    }

    PDFutureStub getAsyncStub() {
      return asyncStub;
    }

    long getCreateTime() {
      return createTime;
    }

    @Override
    public String toString() {
      return "[leaderAddr: " + leaderAddr + ", proxyAddr: " + proxyAddr + "]";
    }
  }

  private Metapb.Region decodeRegion(Metapb.Region region) {
    final boolean isRawRegion = conf.getKvMode() == KVMode.RAW;
    Metapb.Region.Builder builder =
        Metapb.Region.newBuilder()
            .setId(region.getId())
            .setRegionEpoch(region.getRegionEpoch())
            .addAllPeers(region.getPeersList());

    if (region.getStartKey().isEmpty() || isRawRegion) {
      builder.setStartKey(region.getStartKey());
    } else {
      byte[] decodedStartKey = BytesCodec.readBytes(new CodecDataInput(region.getStartKey()));
      builder.setStartKey(ByteString.copyFrom(decodedStartKey));
    }

    if (region.getEndKey().isEmpty() || isRawRegion) {
      builder.setEndKey(region.getEndKey());
    } else {
      byte[] decodedEndKey = BytesCodec.readBytes(new CodecDataInput(region.getEndKey()));
      builder.setEndKey(ByteString.copyFrom(decodedEndKey));
    }

    return builder.build();
  }

  private static class SeekLeaderPDTask {
    private final ListenableFuture<GetMembersResponse> task;
    private final String addr;

    private SeekLeaderPDTask(ListenableFuture<GetMembersResponse> task, String addr) {
      this.task = task;
      this.addr = addr;
    }
  }

  private static class SeekProxyPDTask {
    private final ListenableFuture<GetMembersResponse> task;
    private final String addr;

    private SeekProxyPDTask(ListenableFuture<GetMembersResponse> task, String addr) {
      this.task = task;
      this.addr = addr;
    }
  }
}
