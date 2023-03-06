/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.tikv.common.operation.PDErrorHandler.getRegionResponseErrorExtractor;
import static org.tikv.common.pd.PDError.buildFromPdpbError;
import static org.tikv.common.pd.PDUtils.addrToUri;
import static org.tikv.common.pd.PDUtils.uriToAddr;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.annotations.VisibleForTesting;
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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.operation.NoopHandler;
import org.tikv.common.operation.PDErrorHandler;
import org.tikv.common.util.BackOffFunction.BackOffFuncType;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.HistogramUtils;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.PDGrpc.PDBlockingStub;
import org.tikv.kvproto.PDGrpc.PDFutureStub;
import org.tikv.kvproto.Pdpb;
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
import org.tikv.kvproto.Pdpb.UpdateServiceGCSafePointRequest;

public class PDClient extends AbstractGRPCClient<PDBlockingStub, PDFutureStub>
    implements ReadOnlyPDClient {
  private static final String TIFLASH_TABLE_SYNC_PROGRESS_PATH = "/tiflash/table/sync";
  private static final long MIN_TRY_UPDATE_DURATION = 50;
  private static final int PAUSE_CHECKER_TIMEOUT = 300; // in seconds
  private static final int KEEP_CHECKER_PAUSE_PERIOD = PAUSE_CHECKER_TIMEOUT / 5; // in seconds
  private static final Logger logger = LoggerFactory.getLogger(PDClient.class);

  private final RequestKeyCodec codec;
  private RequestHeader header;
  private TsoRequest tsoReq;
  private volatile PDClientWrapper pdClientWrapper;
  private ScheduledExecutorService service;
  private ScheduledExecutorService tiflashReplicaService;
  private final HashMap<PDChecker, ScheduledExecutorService> pauseCheckerService = new HashMap<>();
  private List<URI> pdAddrs;
  private Client etcdClient;
  private ConcurrentMap<Long, Double> tiflashReplicaMap;
  private HostMapping hostMapping;
  private long lastUpdateLeaderTime;
  private final ExecutorService updateLeaderService = Executors.newSingleThreadExecutor();
  private final AtomicBoolean updateLeaderNotify = new AtomicBoolean();

  public static final Histogram PD_GET_REGION_BY_KEY_REQUEST_LATENCY =
      HistogramUtils.buildDuration()
          .name("client_java_pd_get_region_by_requests_latency")
          .help("pd getRegionByKey request latency.")
          .labelNames("cluster")
          .register();

  private PDClient(TiConfiguration conf, RequestKeyCodec codec, ChannelFactory channelFactory) {
    super(conf, channelFactory);
    initCluster();
    this.codec = codec;
    this.blockingStub = getBlockingStub();
    this.asyncStub = getAsyncStub();
  }

  public static ReadOnlyPDClient create(
      TiConfiguration conf, RequestKeyCodec codec, ChannelFactory channelFactory) {
    return createRaw(conf, codec, channelFactory);
  }

  static PDClient createRaw(
      TiConfiguration conf, RequestKeyCodec codec, ChannelFactory channelFactory) {
    return new PDClient(conf, codec, channelFactory);
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

  public synchronized void keepPauseChecker(PDChecker checker) {
    if (!this.pauseCheckerService.containsKey(checker)) {
      ScheduledExecutorService newService =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat(String.format("PDClient-pause-%s-pool-%%d", checker.name()))
                  .setDaemon(true)
                  .build());
      newService.scheduleAtFixedRate(
          () -> pauseChecker(checker, PAUSE_CHECKER_TIMEOUT),
          0,
          KEEP_CHECKER_PAUSE_PERIOD,
          TimeUnit.SECONDS);
      this.pauseCheckerService.put(checker, newService);
    }
  }

  public synchronized void stopKeepPauseChecker(PDChecker checker) {
    if (this.pauseCheckerService.containsKey(checker)) {
      this.pauseCheckerService.get(checker).shutdown();
      this.pauseCheckerService.remove(checker);
    }
  }

  public void resumeChecker(PDChecker checker) {
    pauseChecker(checker, 0);
  }

  private void pauseChecker(PDChecker checker, int timeout) {
    String verb = timeout == 0 ? "resume" : "pause";
    URI url = pdAddrs.get(0);
    String api = url.toString() + "/pd/api/v1/checker/" + checker.apiName();
    HashMap<String, Integer> arguments = new HashMap<>();
    arguments.put("delay", timeout);
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      JsonMapper jsonMapper = new JsonMapper();
      byte[] body = jsonMapper.writeValueAsBytes(arguments);
      HttpPost post = new HttpPost(api);
      post.setEntity(new ByteArrayEntity(body));
      try (CloseableHttpResponse resp = client.execute(post)) {
        if (resp.getStatusLine().getStatusCode() != 200) {
          logger.error("failed to {} checker.", verb);
        }
        logger.info("checker {} {}d", checker.apiName(), verb);
      }
    } catch (Exception e) {
      logger.error(String.format("failed to %s checker.", verb), e);
    }
  }

  public Boolean isCheckerPaused(PDChecker checker) {
    URI url = pdAddrs.get(0);
    String api = url.toString() + "/pd/api/v1/checker/" + checker.apiName();
    try {
      ObjectMapper mapper = new ObjectMapper();
      HashMap<String, Boolean> status =
          mapper.readValue(new URL(api), new TypeReference<HashMap<String, Boolean>>() {});
      return status.get("paused");
    } catch (Exception e) {
      logger.error(String.format("failed to get %s checker status.", checker.apiName()), e);
      return null;
    }
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
                  resp.getStatus()));
        }
      }
    }
  }

  private GetOperatorResponse getOperator(long regionId) {
    Supplier<GetOperatorRequest> request =
        () -> GetOperatorRequest.newBuilder().setHeader(header).setRegionId(regionId).build();
    // get operator no need to handle error and no need back offer.
    return callWithRetry(
        ConcreteBackOffer.newCustomBackOff(0, getClusterId()),
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
    Histogram.Timer requestTimer =
        PD_GET_REGION_BY_KEY_REQUEST_LATENCY.labels(getClusterId().toString()).startTimer();
    try {
      Supplier<GetRegionRequest> request =
          () ->
              GetRegionRequest.newBuilder()
                  .setHeader(header)
                  .setRegionKey(codec.encodePdQuery(key))
                  .build();

      PDErrorHandler<GetRegionResponse> handler =
          new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

      GetRegionResponse resp =
          callWithRetry(backOffer, PDGrpc.getGetRegionMethod(), request, handler);
      return new Pair<>(codec.decodeRegion(resp.getRegion()), resp.getLeader());
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
    return new Pair<Metapb.Region, Metapb.Peer>(
        codec.decodeRegion(resp.getRegion()), resp.getLeader());
  }

  @Override
  public List<Pdpb.Region> scanRegions(
      BackOffer backOffer, ByteString startKey, ByteString endKey, int limit) {
    // no need to backoff because ScanRegions is just for optimization
    // introduce a warm-up timeout for ScanRegions requests
    PDGrpc.PDBlockingStub stub =
        getBlockingStub().withDeadlineAfter(conf.getWarmUpTimeout(), TimeUnit.MILLISECONDS);
    Pair<ByteString, ByteString> range = codec.encodePdQueryRange(startKey, endKey);
    Pdpb.ScanRegionsRequest request =
        Pdpb.ScanRegionsRequest.newBuilder()
            .setHeader(header)
            .setStartKey(range.first)
            .setEndKey(range.second)
            .setLimit(limit)
            .build();
    Pdpb.ScanRegionsResponse resp = stub.scanRegions(request);
    if (resp == null) {
      return null;
    }

    return codec.decodePdRegions(resp.getRegionsList());
  }

  private Supplier<GetStoreRequest> buildGetStoreReq(long storeId) {
    return () -> GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
  }

  private Supplier<GetAllStoresRequest> buildGetAllStoresReq() {
    return () -> GetAllStoresRequest.newBuilder().setHeader(header).build();
  }

  private Supplier<UpdateServiceGCSafePointRequest> buildUpdateServiceGCSafePointRequest(
      ByteString serviceId, long ttl, long safePoint) {
    return () ->
        UpdateServiceGCSafePointRequest.newBuilder()
            .setHeader(header)
            .setSafePoint(safePoint)
            .setServiceId(serviceId)
            .setTTL(ttl)
            .build();
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
  public Long updateServiceGCSafePoint(
      String serviceId, long ttl, long safePoint, BackOffer backOffer) {
    return callWithRetry(
            backOffer,
            PDGrpc.getUpdateServiceGCSafePointMethod(),
            buildUpdateServiceGCSafePointRequest(ByteString.copyFromUtf8(serviceId), ttl, safePoint),
            new PDErrorHandler<>(
                r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
                this))
        .getMinSafePoint();
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

    updateLeaderService.shutdownNow();
  }

  @VisibleForTesting
  RequestHeader getHeader() {
    return header;
  }

  @VisibleForTesting
  PDClientWrapper getPdClientWrapper() {
    return pdClientWrapper;
  }

  private GetMembersResponse doGetMembers(BackOffer backOffer, URI uri) {
    while (true) {
      backOffer.checkTimeout();

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
        logger.warn(
            "failed to get member from pd server from {}, caused by: {}", uri, e.getMessage());
        backOffer.doBackOff(BackOffFuncType.BoPDRPC, e);
      }
    }
  }

  private GetMembersResponse getMembers(BackOffer backOffer, URI uri) {
    try {
      return doGetMembers(backOffer, uri);
    } catch (Exception e) {
      return null;
    }
  }

  // return whether the leader has changed to target address `leaderUrlStr`.
  synchronized boolean trySwitchLeader(String leaderUrlStr) {
    if (pdClientWrapper != null) {
      if (leaderUrlStr.equals(pdClientWrapper.getLeaderInfo())) {
        // The message to leader is not forwarded by follower.
        if (leaderUrlStr.equals(pdClientWrapper.getStoreAddress())) {
          return true;
        }
      }
      // If leader has transferred to another member, we can create another leaderWrapper.
    }
    // switch leader
    return createLeaderClientWrapper(leaderUrlStr);
  }

  private synchronized boolean createLeaderClientWrapper(String leaderUrlStr) {
    try {
      // create new Leader
      ManagedChannel clientChannel = channelFactory.getChannel(leaderUrlStr, hostMapping);
      pdClientWrapper =
          new PDClientWrapper(leaderUrlStr, leaderUrlStr, clientChannel, System.nanoTime());
      timeout = conf.getTimeout();
    } catch (IllegalArgumentException e) {
      return false;
    }
    logger.info(String.format("Switched to new leader: %s", pdClientWrapper));
    return true;
  }

  synchronized boolean createFollowerClientWrapper(
      BackOffer backOffer, String followerUrlStr, String leaderUrls) {
    // TODO: Why not strip protocol info on server side since grpc does not need it

    try {
      if (!checkHealth(backOffer, followerUrlStr, hostMapping)) {
        return false;
      }

      // create new Leader
      ManagedChannel channel = channelFactory.getChannel(followerUrlStr, hostMapping);
      pdClientWrapper = new PDClientWrapper(leaderUrls, followerUrlStr, channel, System.nanoTime());
      timeout = conf.getForwardTimeout();
    } catch (IllegalArgumentException e) {
      return false;
    }
    logger.info(String.format("Switched to new leader by follower forward: %s", pdClientWrapper));
    return true;
  }

  public void tryUpdateLeaderOrForwardFollower() {
    if (updateLeaderNotify.compareAndSet(false, true)) {
      try {
        updateLeaderService.submit(
            () -> {
              try {
                updateLeaderOrForwardFollower();
              } catch (Exception e) {
                logger.info("update leader or forward follower failed", e);
                throw e;
              } finally {
                updateLeaderNotify.set(false);
                logger.info("updating leader finish");
              }
            });
      } catch (RejectedExecutionException e) {
        logger.error("PDClient is shutdown", e);
        updateLeaderNotify.set(false);
      }
    }
  }

  private synchronized void updateLeaderOrForwardFollower() {
    logger.warn("updating leader or forward follower");
    if (System.currentTimeMillis() - lastUpdateLeaderTime < MIN_TRY_UPDATE_DURATION) {
      return;
    }
    for (URI url : this.pdAddrs) {
      BackOffer backOffer = this.probeBackOffer();
      // since resp is null, we need update leader's address by walking through all pd server.
      GetMembersResponse resp = getMembers(backOffer, url);
      if (resp == null) {
        continue;
      }
      if (resp.getLeader().getClientUrlsList().isEmpty()) {
        continue;
      }

      String leaderUrlStr = resp.getLeader().getClientUrlsList().get(0);
      leaderUrlStr = uriToAddr(addrToUri(leaderUrlStr));

      // if leader is switched, just return.
      if (checkHealth(backOffer, leaderUrlStr, hostMapping)
          && createLeaderClientWrapper(leaderUrlStr)) {
        lastUpdateLeaderTime = System.currentTimeMillis();
        return;
      }

      if (!conf.getEnableGrpcForward()) {
        continue;
      }

      logger.info(String.format("can not switch to new leader, try follower forward"));
      List<Pdpb.Member> members = resp.getMembersList();

      // If we have not used follower forward, try the first follower.
      boolean hasReachNextMember =
          pdClientWrapper != null && pdClientWrapper.getStoreAddress().equals(leaderUrlStr);

      for (int i = 0; i < members.size() * 2; i++) {
        Pdpb.Member member = members.get(i % members.size());
        if (member.getMemberId() == resp.getLeader().getMemberId()) {
          continue;
        }
        String followerUrlStr = member.getClientUrlsList().get(0);
        followerUrlStr = uriToAddr(addrToUri(followerUrlStr));
        if (pdClientWrapper != null && pdClientWrapper.getStoreAddress().equals(followerUrlStr)) {
          hasReachNextMember = true;
          continue;
        }
        if (hasReachNextMember
            && createFollowerClientWrapper(backOffer, followerUrlStr, leaderUrlStr)) {
          logger.warn(
              String.format("forward request to pd [%s] by pd [%s]", leaderUrlStr, followerUrlStr));
          return;
        }
      }
    }
    lastUpdateLeaderTime = System.currentTimeMillis();
    if (pdClientWrapper == null) {
      throw new TiClientInternalException(
          "already tried all address on file, but not leader found yet.");
    }
  }

  public void tryUpdateLeader() {
    logger.info("try update leader");
    for (URI url : this.pdAddrs) {
      BackOffer backOffer = this.probeBackOffer();
      // since resp is null, we need update leader's address by walking through all pd server.
      GetMembersResponse resp = getMembers(backOffer, url);
      if (resp == null) {
        continue;
      }
      List<URI> urls =
          resp.getMembersList()
              .stream()
              .map(mem -> addrToUri(mem.getClientUrls(0)))
              .collect(Collectors.toList());
      String leaderUrlStr = resp.getLeader().getClientUrlsList().get(0);
      leaderUrlStr = uriToAddr(addrToUri(leaderUrlStr));

      // If leader is not change but becomes available, we can cancel follower forward.
      if (checkHealth(backOffer, leaderUrlStr, hostMapping) && trySwitchLeader(leaderUrlStr)) {
        if (!urls.equals(this.pdAddrs)) {
          tryUpdateMembers(urls);
        }
        return;
      }
    }
    lastUpdateLeaderTime = System.currentTimeMillis();
    if (pdClientWrapper == null) {
      throw new TiClientInternalException(
          "already tried all address on file, but not leader found yet.");
    }
  }

  private synchronized void tryUpdateMembers(List<URI> members) {
    this.pdAddrs = members;
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
    return pdClientWrapper.getBlockingStub().withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected PDFutureStub getAsyncStub() {
    if (pdClientWrapper == null) {
      throw new GrpcException("PDClient may not be initialized");
    }
    return pdClientWrapper.getAsyncStub().withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
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
    this.timeout = conf.getPdFirstGetMemberTimeout();
    for (URI u : pdAddrs) {
      logger.info("get members with pd " + u + ": start");
      resp = getMembers(defaultBackOffer(), u);
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
            tryUpdateLeader();
          } catch (Exception e) {
            logger.warn("Update leader failed", e);
          }
        },
        10,
        10,
        TimeUnit.SECONDS);
    if (conf.isTiFlashEnabled()) {
      tiflashReplicaService =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat("PDClient-tiflash-replica-pool-%d")
                  .setDaemon(true)
                  .build());
      tiflashReplicaService.scheduleAtFixedRate(
          this::updateTiFlashReplicaStatus, 10, 10, TimeUnit.SECONDS);
    }
    logger.info("init cluster: finish");
  }

  static class PDClientWrapper {
    private final String leaderInfo;
    private final PDBlockingStub blockingStub;
    private final PDFutureStub asyncStub;
    private final long createTime;
    private final String storeAddress;

    PDClientWrapper(
        String leaderInfo, String storeAddress, ManagedChannel clientChannel, long createTime) {
      if (!storeAddress.equals(leaderInfo)) {
        Metadata header = new Metadata();
        header.put(TiConfiguration.PD_FORWARD_META_DATA_KEY, addrToUri(leaderInfo).toString());
        this.blockingStub =
            MetadataUtils.attachHeaders(PDGrpc.newBlockingStub(clientChannel), header);
        this.asyncStub = MetadataUtils.attachHeaders(PDGrpc.newFutureStub(clientChannel), header);
      } else {
        this.blockingStub = PDGrpc.newBlockingStub(clientChannel);
        this.asyncStub = PDGrpc.newFutureStub(clientChannel);
      }
      this.leaderInfo = leaderInfo;
      this.storeAddress = storeAddress;
      this.createTime = createTime;
    }

    String getLeaderInfo() {
      return leaderInfo;
    }

    String getStoreAddress() {
      return storeAddress;
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
      return "[leaderInfo: " + leaderInfo + ", storeAddress: " + storeAddress + "]";
    }
  }

  public Long getClusterId() {
    return header.getClusterId();
  }

  public List<URI> getPdAddrs() {
    return pdAddrs;
  }

  public RequestKeyCodec getCodec() {
    return codec;
  }

  private static BackOffer defaultBackOffer() {
    return ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF);
  }

  private BackOffer probeBackOffer() {
    int maxSleep = (int) getTimeout() * 2;
    return ConcreteBackOffer.newCustomBackOff(maxSleep);
  }
}
