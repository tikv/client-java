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

package org.tikv.common.region;

import static org.tikv.common.region.RegionStoreClient.RequestTypes.REQ_TYPE_DAG;
import static org.tikv.common.util.BackOffFunction.BackOffFuncType.BoRegionMiss;
import static org.tikv.common.util.BackOffFunction.BackOffFuncType.BoTxnLock;
import static org.tikv.common.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.PDClient;
import org.tikv.common.StoreVersion;
import org.tikv.common.TiConfiguration;
import org.tikv.common.Version;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.KeyException;
import org.tikv.common.exception.RawCASConflictException;
import org.tikv.common.exception.RegionException;
import org.tikv.common.exception.SelectException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.log.SlowLogEmptyImpl;
import org.tikv.common.operation.KVErrorHandler;
import org.tikv.common.operation.RegionErrorHandler;
import org.tikv.common.streaming.StreamingResponse;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.Batch;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.HistogramUtils;
import org.tikv.common.util.Pair;
import org.tikv.common.util.RangeSplitter;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb.BatchGetRequest;
import org.tikv.kvproto.Kvrpcpb.BatchGetResponse;
import org.tikv.kvproto.Kvrpcpb.CommitRequest;
import org.tikv.kvproto.Kvrpcpb.CommitResponse;
import org.tikv.kvproto.Kvrpcpb.GetRequest;
import org.tikv.kvproto.Kvrpcpb.GetResponse;
import org.tikv.kvproto.Kvrpcpb.KeyError;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Kvrpcpb.Mutation;
import org.tikv.kvproto.Kvrpcpb.PrewriteRequest;
import org.tikv.kvproto.Kvrpcpb.PrewriteResponse;
import org.tikv.kvproto.Kvrpcpb.RawBatchDeleteRequest;
import org.tikv.kvproto.Kvrpcpb.RawBatchDeleteResponse;
import org.tikv.kvproto.Kvrpcpb.RawBatchGetRequest;
import org.tikv.kvproto.Kvrpcpb.RawBatchGetResponse;
import org.tikv.kvproto.Kvrpcpb.RawBatchPutRequest;
import org.tikv.kvproto.Kvrpcpb.RawBatchPutResponse;
import org.tikv.kvproto.Kvrpcpb.RawCASRequest;
import org.tikv.kvproto.Kvrpcpb.RawCASResponse;
import org.tikv.kvproto.Kvrpcpb.RawDeleteRangeRequest;
import org.tikv.kvproto.Kvrpcpb.RawDeleteRangeResponse;
import org.tikv.kvproto.Kvrpcpb.RawDeleteRequest;
import org.tikv.kvproto.Kvrpcpb.RawDeleteResponse;
import org.tikv.kvproto.Kvrpcpb.RawGetKeyTTLRequest;
import org.tikv.kvproto.Kvrpcpb.RawGetKeyTTLResponse;
import org.tikv.kvproto.Kvrpcpb.RawGetRequest;
import org.tikv.kvproto.Kvrpcpb.RawGetResponse;
import org.tikv.kvproto.Kvrpcpb.RawPutRequest;
import org.tikv.kvproto.Kvrpcpb.RawPutResponse;
import org.tikv.kvproto.Kvrpcpb.RawScanRequest;
import org.tikv.kvproto.Kvrpcpb.RawScanResponse;
import org.tikv.kvproto.Kvrpcpb.ScanRequest;
import org.tikv.kvproto.Kvrpcpb.ScanResponse;
import org.tikv.kvproto.Kvrpcpb.SplitRegionRequest;
import org.tikv.kvproto.Kvrpcpb.SplitRegionResponse;
import org.tikv.kvproto.Kvrpcpb.TxnHeartBeatRequest;
import org.tikv.kvproto.Kvrpcpb.TxnHeartBeatResponse;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvFutureStub;
import org.tikv.txn.AbstractLockResolverClient;
import org.tikv.txn.Lock;
import org.tikv.txn.ResolveLockResult;
import org.tikv.txn.exception.LockException;

// TODO:
//  1. RegionStoreClient will be inaccessible directly.
//  2. All apis of RegionStoreClient would not provide retry aside from callWithRetry,
//  if a request needs to be retried because of an un-retryable cause, e.g., keys
//  need to be re-split across regions/stores, region info outdated, e.t.c., you
//  should retry it in an upper client logic (KVClient, TxnClient, e.t.c.)

/** Note that RegionStoreClient itself is not thread-safe */
public class RegionStoreClient extends AbstractRegionStoreClient {
  private static final Logger logger = LoggerFactory.getLogger(RegionStoreClient.class);
  @VisibleForTesting public final AbstractLockResolverClient lockResolverClient;
  private final TiStoreType storeType;
  /** startTS -> List(locks) */
  private final Map<Long, Set<Long>> resolvedLocks = new HashMap<>();

  private final PDClient pdClient;
  private Boolean isV4 = null;

  public static final Histogram GRPC_RAW_REQUEST_LATENCY =
      HistogramUtils.buildDuration()
          .name("client_java_grpc_raw_requests_latency")
          .help("grpc raw request latency.")
          .labelNames("type", "cluster")
          .register();

  private synchronized Boolean getIsV4() {
    if (isV4 == null) {
      isV4 = StoreVersion.minTiKVVersion(Version.RESOLVE_LOCK_V4, pdClient);
    }
    return isV4;
  }

  private RegionStoreClient(
      TiConfiguration conf,
      TiRegion region,
      TiStore store,
      TiStoreType storeType,
      ChannelFactory channelFactory,
      TikvBlockingStub blockingStub,
      TikvFutureStub asyncStub,
      RegionManager regionManager,
      PDClient pdClient,
      RegionStoreClient.RegionStoreClientBuilder clientBuilder) {
    super(conf, region, store, channelFactory, blockingStub, asyncStub, regionManager);
    this.storeType = storeType;

    if (this.storeType == TiStoreType.TiKV) {
      this.lockResolverClient =
          AbstractLockResolverClient.getInstance(
              conf,
              region,
              store,
              this.blockingStub,
              this.asyncStub,
              channelFactory,
              regionManager,
              pdClient,
              clientBuilder);

    } else {
      TiStore tikvStore =
          regionManager.getRegionStorePairByKey(region.getStartKey(), TiStoreType.TiKV).second;

      String addressStr = tikvStore.getStore().getAddress();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Create region store client on address %s", addressStr));
      }
      ManagedChannel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());

      TikvBlockingStub tikvBlockingStub = TikvGrpc.newBlockingStub(channel);
      TikvGrpc.TikvFutureStub tikvAsyncStub = TikvGrpc.newFutureStub(channel);

      this.lockResolverClient =
          AbstractLockResolverClient.getInstance(
              conf,
              region,
              tikvStore,
              tikvBlockingStub,
              tikvAsyncStub,
              channelFactory,
              regionManager,
              pdClient,
              clientBuilder);
    }
    this.pdClient = pdClient;
  }

  public synchronized boolean addResolvedLocks(Long version, Set<Long> locks) {
    Set<Long> oldList = resolvedLocks.get(version);
    if (oldList != null) {
      oldList.addAll(locks);
    } else {
      resolvedLocks.put(version, new HashSet<>(locks));
    }
    return true;
  }

  public synchronized Set<Long> getResolvedLocks(Long version) {
    return resolvedLocks.getOrDefault(version, java.util.Collections.emptySet());
  }

  /**
   * Fetch a value according to a key
   *
   * @param backOffer backOffer
   * @param key key to fetch
   * @param version key version
   * @return value
   * @throws TiClientInternalException TiSpark Client exception, unexpected
   * @throws KeyException Key may be locked
   */
  public ByteString get(BackOffer backOffer, ByteString key, long version)
      throws TiClientInternalException, KeyException {
    boolean forWrite = false;
    Supplier<GetRequest> factory =
        () ->
            GetRequest.newBuilder()
                .setContext(
                    makeContext(getResolvedLocks(version), this.storeType, backOffer.getSlowLog()))
                .setKey(codec.encodeKey(key))
                .setVersion(version)
                .build();

    KVErrorHandler<GetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> resp.hasError() ? resp.getError() : null,
            resolveLockResult -> addResolvedLocks(version, resolveLockResult.getResolvedLocks()),
            version,
            forWrite);

    GetResponse resp = callWithRetry(backOffer, TikvGrpc.getKvGetMethod(), factory, handler);

    handleGetResponse(resp);
    return resp.getValue();
  }

  /**
   * @param resp GetResponse
   * @throws TiClientInternalException TiSpark Client exception, unexpected
   * @throws KeyException Key may be locked
   */
  private void handleGetResponse(GetResponse resp) throws TiClientInternalException, KeyException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("GetResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    if (resp.hasError()) {
      throw new KeyException(resp.getError());
    }
  }

  public List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version) {
    boolean forWrite = false;
    Supplier<BatchGetRequest> request =
        () ->
            BatchGetRequest.newBuilder()
                .setContext(
                    makeContext(getResolvedLocks(version), this.storeType, backOffer.getSlowLog()))
                .addAllKeys(codec.encodeKeys(keys))
                .setVersion(version)
                .build();
    KVErrorHandler<BatchGetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> null,
            resolveLockResult -> addResolvedLocks(version, resolveLockResult.getResolvedLocks()),
            version,
            forWrite);
    BatchGetResponse resp =
        callWithRetry(backOffer, TikvGrpc.getKvBatchGetMethod(), request, handler);
    return handleBatchGetResponse(backOffer, resp, version);
  }

  private List<KvPair> handleBatchGetResponse(
      BackOffer backOffer, BatchGetResponse resp, long version) {
    boolean forWrite = false;
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("BatchGetResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    List<Lock> locks = new ArrayList<>();

    for (KvPair pair : resp.getPairsList()) {
      if (pair.hasError()) {
        if (pair.getError().hasLocked()) {
          Lock lock = new Lock(pair.getError().getLocked(), codec);
          locks.add(lock);
        } else {
          throw new KeyException(pair.getError());
        }
      }
    }

    if (!locks.isEmpty()) {
      ResolveLockResult resolveLockResult =
          lockResolverClient.resolveLocks(backOffer, version, locks, forWrite);
      addResolvedLocks(version, resolveLockResult.getResolvedLocks());
      // resolveLocks already retried, just throw error to upper logic.
      throw new TiKVException("locks not resolved, retry");
    }

    return codec.decodeKvPairs(resp.getPairsList());
  }

  public List<KvPair> scan(
      BackOffer backOffer, ByteString startKey, long version, boolean keyOnly) {
    boolean forWrite = false;
    while (true) {
      Supplier<ScanRequest> request =
          () ->
              ScanRequest.newBuilder()
                  .setContext(
                      makeContext(
                          getResolvedLocks(version), this.storeType, backOffer.getSlowLog()))
                  .setStartKey(codec.encodeKey(startKey))
                  .setVersion(version)
                  .setKeyOnly(keyOnly)
                  .setLimit(getConf().getScanBatchSize())
                  .build();

      KVErrorHandler<ScanResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null,
              resolveLockResult -> addResolvedLocks(version, resolveLockResult.getResolvedLocks()),
              version,
              forWrite);
      ScanResponse resp = callWithRetry(backOffer, TikvGrpc.getKvScanMethod(), request, handler);
      // retry may refresh region info
      // we need to update region after retry
      region = regionManager.getRegionByKey(startKey, backOffer);

      if (isScanSuccess(backOffer, resp)) {
        return doScan(resp);
      }
    }
  }

  private boolean isScanSuccess(BackOffer backOffer, ScanResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("ScanResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      backOffer.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
      return false;
    }
    return true;
  }

  // TODO: resolve locks after scan
  private List<KvPair> doScan(ScanResponse resp) {
    // Check if kvPair contains error, it should be a Lock if hasError is true.
    List<KvPair> kvPairs = resp.getPairsList();
    List<KvPair> newKvPairs = new ArrayList<>();
    for (KvPair kvPair : kvPairs) {
      if (kvPair.hasError()) {
        Lock lock = AbstractLockResolverClient.extractLockFromKeyErr(kvPair.getError(), codec);
        newKvPairs.add(
            KvPair.newBuilder()
                .setError(kvPair.getError())
                .setValue(kvPair.getValue())
                .setKey(lock.getKey())
                .build());
      } else {
        newKvPairs.add(codec.decodeKvPair(kvPair));
      }
    }
    return Collections.unmodifiableList(newKvPairs);
  }

  public List<KvPair> scan(BackOffer backOffer, ByteString startKey, long version) {
    return scan(backOffer, startKey, version, false);
  }

  /**
   * Prewrite batch keys
   *
   * @param backOffer backOffer
   * @param primary primary lock of keys
   * @param mutations batch key-values as mutations
   * @param startTs startTs of prewrite
   * @param lockTTL lock ttl
   * @throws TiClientInternalException TiSpark Client exception, unexpected
   * @throws KeyException Key may be locked
   * @throws RegionException region error occurs
   */
  public void prewrite(
      BackOffer backOffer, ByteString primary, List<Mutation> mutations, long startTs, long lockTTL)
      throws TiClientInternalException, KeyException, RegionException {
    this.prewrite(backOffer, primary, mutations, startTs, lockTTL, false);
  }

  /**
   * Prewrite batch keys
   *
   * @param skipConstraintCheck whether to skip constraint check
   */
  public void prewrite(
      BackOffer bo,
      ByteString primaryLock,
      List<Mutation> mutations,
      long startTs,
      long ttl,
      boolean skipConstraintCheck)
      throws TiClientInternalException, KeyException, RegionException {
    boolean forWrite = true;
    while (true) {
      Supplier<PrewriteRequest> factory =
          () ->
              getIsV4()
                  ? PrewriteRequest.newBuilder()
                      .setContext(makeContext(storeType, bo.getSlowLog()))
                      .setStartVersion(startTs)
                      .setPrimaryLock(codec.encodeKey(primaryLock))
                      .addAllMutations(codec.encodeMutations(mutations))
                      .setLockTtl(ttl)
                      .setSkipConstraintCheck(skipConstraintCheck)
                      .setMinCommitTs(startTs)
                      .setTxnSize(16)
                      .build()
                  : PrewriteRequest.newBuilder()
                      .setContext(makeContext(storeType, bo.getSlowLog()))
                      .setStartVersion(startTs)
                      .setPrimaryLock(primaryLock)
                      .addAllMutations(mutations)
                      .setLockTtl(ttl)
                      .setSkipConstraintCheck(skipConstraintCheck)
                      // v3 does not support setMinCommitTs(startTs)
                      .setTxnSize(16)
                      .build();
      KVErrorHandler<PrewriteResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null,
              resolveLockResult -> null,
              startTs,
              forWrite);
      PrewriteResponse resp = callWithRetry(bo, TikvGrpc.getKvPrewriteMethod(), factory, handler);
      if (isPrewriteSuccess(bo, resp, startTs)) {
        return;
      }
    }
  }

  /**
   * @param backOffer backOffer
   * @param resp response
   * @return Return true means the rpc call success. Return false means the rpc call fail,
   *     RegionStoreClient should retry. Throw an Exception means the rpc call fail,
   *     RegionStoreClient cannot handle this kind of error
   * @throws TiClientInternalException
   * @throws RegionException
   * @throws KeyException
   */
  private boolean isPrewriteSuccess(BackOffer backOffer, PrewriteResponse resp, long startTs)
      throws TiClientInternalException, KeyException, RegionException {
    boolean forWrite = true;
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("Prewrite Response failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }

    boolean isSuccess = true;
    List<Lock> locks = new ArrayList<>();
    for (KeyError err : resp.getErrorsList()) {
      if (err.hasLocked()) {
        isSuccess = false;
        Lock lock = new Lock(err.getLocked(), codec);
        locks.add(lock);
      } else {
        throw new KeyException(err.toString());
      }
    }
    if (isSuccess) {
      return true;
    }

    ResolveLockResult resolveLockResult =
        lockResolverClient.resolveLocks(backOffer, startTs, locks, forWrite);
    addResolvedLocks(startTs, resolveLockResult.getResolvedLocks());
    long msBeforeExpired = resolveLockResult.getMsBeforeTxnExpired();
    if (msBeforeExpired > 0) {
      backOffer.doBackOffWithMaxSleep(
          BoTxnLock, msBeforeExpired, new KeyException(resp.getErrorsList().get(0)));
    }
    return false;
  }

  /** TXN Heart Beat: update primary key ttl */
  public void txnHeartBeat(BackOffer bo, ByteString primaryLock, long startTs, long ttl) {
    boolean forWrite = false;
    while (true) {
      Supplier<TxnHeartBeatRequest> factory =
          () ->
              TxnHeartBeatRequest.newBuilder()
                  .setContext(makeContext(storeType, bo.getSlowLog()))
                  .setStartVersion(startTs)
                  .setPrimaryLock(primaryLock)
                  .setAdviseLockTtl(ttl)
                  .build();
      KVErrorHandler<TxnHeartBeatResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> resp.hasError() ? resp.getError() : null,
              resolveLockResult -> null,
              startTs,
              forWrite);
      TxnHeartBeatResponse resp =
          callWithRetry(bo, TikvGrpc.getKvTxnHeartBeatMethod(), factory, handler);
      if (isTxnHeartBeatSuccess(resp)) {
        return;
      }
    }
  }

  private boolean isTxnHeartBeatSuccess(TxnHeartBeatResponse resp)
      throws TiClientInternalException, RegionException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("TxnHeartBeat Response failed without a cause");
    }

    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }

    if (resp.hasError()) {
      throw new TiClientInternalException("TxnHeartBeat fail, " + resp.getError().getAbort());
    }

    return true;
  }

  /**
   * Commit batch keys
   *
   * @param backOffer backOffer
   * @param keys keys to commit
   * @param startTs start version
   * @param commitTs commit version
   */
  public void commit(BackOffer backOffer, Iterable<ByteString> keys, long startTs, long commitTs)
      throws KeyException {
    boolean forWrite = true;
    Supplier<CommitRequest> factory =
        () ->
            CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addAllKeys(
                    StreamSupport.stream(keys.spliterator(), false)
                        .map(codec::encodeKey)
                        .collect(Collectors.toList()))
                .setContext(makeContext(storeType, backOffer.getSlowLog()))
                .build();
    KVErrorHandler<CommitResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> resp.hasError() ? resp.getError() : null,
            resolveLockResult -> null,
            startTs,
            forWrite);
    CommitResponse resp = callWithRetry(backOffer, TikvGrpc.getKvCommitMethod(), factory, handler);
    handleCommitResponse(resp);
  }

  /**
   * @param resp CommitResponse
   * @throws TiClientInternalException
   * @throws RegionException
   * @throws KeyException
   */
  private void handleCommitResponse(CommitResponse resp)
      throws TiClientInternalException, RegionException, KeyException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("CommitResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      // bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
      // return false;
      // Caller method should restart commit
      throw new RegionException(resp.getRegionError());
    }
    // If we find locks, we first resolve and let its caller retry.
    if (resp.hasError()) {
      throw new KeyException(resp.getError());
    }
  }

  /**
   * Execute and retrieve the response from TiKV server.
   *
   * @param req Select request to process
   * @param ranges Key range list
   * @return Remaining tasks of this request, if task split happens, null otherwise
   */
  public List<RangeSplitter.RegionTask> coprocess(
      BackOffer backOffer,
      DAGRequest req,
      List<Coprocessor.KeyRange> ranges,
      Queue<SelectResponse> responseQueue,
      long startTs) {
    boolean forWrite = false;
    if (req == null || ranges == null || req.getExecutorsCount() < 1) {
      throw new IllegalArgumentException("Invalid coprocessor argument!");
    }

    Supplier<Coprocessor.Request> reqToSend =
        () ->
            Coprocessor.Request.newBuilder()
                .setContext(
                    makeContext(getResolvedLocks(startTs), this.storeType, backOffer.getSlowLog()))
                .setTp(REQ_TYPE_DAG.getValue())
                .setStartTs(startTs)
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();

    // we should handle the region error ourselves
    KVErrorHandler<Coprocessor.Response> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> null,
            resolveLockResult -> addResolvedLocks(startTs, resolveLockResult.getResolvedLocks()),
            startTs,
            forWrite);
    Coprocessor.Response resp =
        callWithRetry(backOffer, TikvGrpc.getCoprocessorMethod(), reqToSend, handler);
    return handleCopResponse(backOffer, resp, ranges, responseQueue, startTs);
  }

  // handleCopResponse checks coprocessor Response for region split and lock,
  // returns more tasks when that happens, or handles the response if no error.
  // if we're handling streaming coprocessor response, lastRange is the range of last
  // successful response, otherwise it's nil.
  private List<RangeSplitter.RegionTask> handleCopResponse(
      BackOffer backOffer,
      Coprocessor.Response response,
      List<Coprocessor.KeyRange> ranges,
      Queue<SelectResponse> responseQueue,
      long startTs) {
    boolean forWrite = false;
    if (response == null) {
      // Send request failed, reasons may:
      // 1. TiKV down
      // 2. Network partition
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss,
          new GrpcException("TiKV down or Network partition"));
      logger.warn("Re-splitting region task due to region error: TiKV down or Network partition");
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges, storeType);
    }

    if (response.hasRegionError()) {
      Errorpb.Error regionError = response.getRegionError();
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(regionError.toString()));
      logger.warn("Re-splitting region task due to region error:" + regionError.getMessage());
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges, storeType);
    }

    if (response.hasLocked()) {
      Lock lock = new Lock(response.getLocked(), codec);
      logger.debug(String.format("coprocessor encounters locks: %s", lock));
      ResolveLockResult resolveLockResult =
          lockResolverClient.resolveLocks(
              backOffer, startTs, Collections.singletonList(lock), forWrite);
      addResolvedLocks(startTs, resolveLockResult.getResolvedLocks());
      long msBeforeExpired = resolveLockResult.getMsBeforeTxnExpired();
      if (msBeforeExpired > 0) {
        backOffer.doBackOffWithMaxSleep(BoTxnLockFast, msBeforeExpired, new LockException(lock));
      }
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges, storeType);
    }

    String otherError = response.getOtherError();
    if (!otherError.isEmpty()) {
      logger.warn(String.format("Other error occurred, message: %s", otherError));
      throw new GrpcException(otherError);
    }

    responseQueue.offer(doCoprocessor(response));
    return null;
  }

  private Iterator<SelectResponse> doCoprocessor(StreamingResponse response) {
    Iterator<Coprocessor.Response> responseIterator = response.iterator();
    // If we got nothing to handle, return null
    if (!responseIterator.hasNext()) {
      return null;
    }

    // Simply wrap it
    return new Iterator<SelectResponse>() {
      @Override
      public boolean hasNext() {
        return responseIterator.hasNext();
      }

      @Override
      public SelectResponse next() {
        return doCoprocessor(responseIterator.next());
      }
    };
  }

  private SelectResponse doCoprocessor(Coprocessor.Response resp) {
    try {
      SelectResponse selectResp = SelectResponse.parseFrom(resp.getData());
      if (selectResp.hasError()) {
        throw new SelectException(selectResp.getError(), selectResp.getError().getMsg());
      }
      return selectResp;
    } catch (InvalidProtocolBufferException e) {
      throw new TiClientInternalException("Error parsing protobuf for coprocessor response.", e);
    }
  }

  // TODO: wait for future fix
  // coprocessStreaming doesn't handle split error
  // future work should handle it and do the resolve
  // locks correspondingly
  public Iterator<SelectResponse> coprocessStreaming(
      DAGRequest req, List<Coprocessor.KeyRange> ranges, long startTs) {
    boolean forWrite = false;
    Supplier<Coprocessor.Request> reqToSend =
        () ->
            Coprocessor.Request.newBuilder()
                .setContext(
                    makeContext(
                        getResolvedLocks(startTs), this.storeType, SlowLogEmptyImpl.INSTANCE))
                // TODO: If no executors...?
                .setTp(REQ_TYPE_DAG.getValue())
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();

    KVErrorHandler<StreamingResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            StreamingResponse::getFirstRegionError, // TODO: handle all errors in streaming response
            resp -> null,
            resolveLockResult -> addResolvedLocks(startTs, resolveLockResult.getResolvedLocks()),
            startTs,
            forWrite);

    StreamingResponse responseIterator =
        this.callServerStreamingWithRetry(
            ConcreteBackOffer.newCopNextMaxBackOff(pdClient.getClusterId()),
            TikvGrpc.getCoprocessorStreamMethod(),
            reqToSend,
            handler);
    return doCoprocessor(responseIterator);
  }

  /**
   * Send SplitRegion request to tikv split a region at splitKey. splitKey must between current
   * region's start key and end key.
   *
   * @param splitKeys is the split points for a specific region.
   * @return a split region info.
   */
  public List<Metapb.Region> splitRegion(List<ByteString> splitKeys) {
    Supplier<SplitRegionRequest> request =
        () ->
            SplitRegionRequest.newBuilder()
                .setContext(makeContext(storeType, SlowLogEmptyImpl.INSTANCE))
                .addAllSplitKeys(codec.encodeKeys(splitKeys))
                .setIsRawKv(conf.isRawKVMode())
                .build();

    KVErrorHandler<SplitRegionResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            null,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> null,
            resolveLockResult -> null,
            0L,
            false);

    SplitRegionResponse resp =
        callWithRetry(
            ConcreteBackOffer.newGetBackOff(pdClient.getClusterId()),
            TikvGrpc.getSplitRegionMethod(),
            request,
            handler);

    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("SplitRegion Response failed without a cause");
    }

    if (resp.hasRegionError()) {
      throw new TiClientInternalException(
          String.format(
              "failed to split region %d because %s", region.getId(), resp.getRegionError()));
    }

    if (conf.getApiVersion().isV1()) {
      return resp.getRegionsList();
    }
    return resp.getRegionsList().stream().map(codec::decodeRegion).collect(Collectors.toList());
  }

  // APIs for Raw Scan/Put/Get/Delete

  public Optional<ByteString> rawGet(BackOffer backOffer, ByteString key) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_get", clusterId.toString()).startTimer();
    try {
      Supplier<RawGetRequest> factory =
          () ->
              RawGetRequest.newBuilder()
                  .setContext(makeContext(storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .build();
      RegionErrorHandler<RawGetResponse> handler =
          new RegionErrorHandler<RawGetResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawGetResponse resp = callWithRetry(backOffer, TikvGrpc.getRawGetMethod(), factory, handler);
      return rawGetHelper(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private Optional<ByteString> rawGetHelper(RawGetResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawGetResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    if (resp.getNotFound()) {
      return Optional.empty();
    } else {
      return Optional.of(resp.getValue());
    }
  }

  public Optional<Long> rawGetKeyTTL(BackOffer backOffer, ByteString key) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_get_key_ttl", clusterId.toString())
            .startTimer();
    try {
      Supplier<RawGetKeyTTLRequest> factory =
          () ->
              RawGetKeyTTLRequest.newBuilder()
                  .setContext(makeContext(storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .build();
      RegionErrorHandler<RawGetKeyTTLResponse> handler =
          new RegionErrorHandler<RawGetKeyTTLResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawGetKeyTTLResponse resp =
          callWithRetry(backOffer, TikvGrpc.getRawGetKeyTTLMethod(), factory, handler);
      return rawGetKeyTTLHelper(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private Optional<Long> rawGetKeyTTLHelper(RawGetKeyTTLResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawGetResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    if (resp.getNotFound()) {
      return Optional.empty();
    }
    return Optional.of(resp.getTtl());
  }

  public void rawDelete(BackOffer backOffer, ByteString key, boolean atomicForCAS) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_delete", clusterId.toString())
            .startTimer();
    try {
      Supplier<RawDeleteRequest> factory =
          () ->
              RawDeleteRequest.newBuilder()
                  .setContext(makeContext(storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setForCas(atomicForCAS)
                  .build();

      RegionErrorHandler<RawDeleteResponse> handler =
          new RegionErrorHandler<RawDeleteResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawDeleteResponse resp =
          callWithRetry(backOffer, TikvGrpc.getRawDeleteMethod(), factory, handler);
      rawDeleteHelper(resp, region);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private void rawDeleteHelper(RawDeleteResponse resp, TiRegion region) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawDeleteResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
  }

  public void rawPut(
      BackOffer backOffer, ByteString key, ByteString value, long ttl, boolean atomicForCAS) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_put", clusterId.toString()).startTimer();
    try {
      Supplier<RawPutRequest> factory =
          () ->
              RawPutRequest.newBuilder()
                  .setContext(makeContext(storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setValue(value)
                  .setTtl(ttl)
                  .setForCas(atomicForCAS)
                  .build();

      RegionErrorHandler<RawPutResponse> handler =
          new RegionErrorHandler<RawPutResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawPutResponse resp = callWithRetry(backOffer, TikvGrpc.getRawPutMethod(), factory, handler);
      rawPutHelper(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private void rawPutHelper(RawPutResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawPutResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
  }

  public void rawCompareAndSet(
      BackOffer backOffer,
      ByteString key,
      Optional<ByteString> prevValue,
      ByteString value,
      long ttl)
      throws RawCASConflictException {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_put_if_absent", clusterId.toString())
            .startTimer();
    try {
      Supplier<RawCASRequest> factory =
          () ->
              RawCASRequest.newBuilder()
                  .setContext(makeContext(storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setValue(value)
                  .setPreviousValue(prevValue.orElse(ByteString.EMPTY))
                  .setPreviousNotExist(!prevValue.isPresent())
                  .setTtl(ttl)
                  .build();

      RegionErrorHandler<RawCASResponse> handler =
          new RegionErrorHandler<RawCASResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawCASResponse resp =
          callWithRetry(backOffer, TikvGrpc.getRawCompareAndSwapMethod(), factory, handler);
      rawCompareAndSetHelper(key, prevValue, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private void rawCompareAndSetHelper(
      ByteString key, Optional<ByteString> expectedPrevValue, RawCASResponse resp)
      throws RawCASConflictException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawCASResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    if (!resp.getSucceed()) {
      if (resp.getPreviousNotExist()) {
        throw new RawCASConflictException(key, expectedPrevValue, Optional.empty());
      } else {
        throw new RawCASConflictException(
            key, expectedPrevValue, Optional.of(resp.getPreviousValue()));
      }
    }
  }

  public List<KvPair> rawBatchGet(BackOffer backoffer, List<ByteString> keys) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_batch_get", clusterId.toString())
            .startTimer();
    try {
      if (keys.isEmpty()) {
        return new ArrayList<>();
      }
      Supplier<RawBatchGetRequest> factory =
          () ->
              RawBatchGetRequest.newBuilder()
                  .setContext(makeContext(storeType, backoffer.getSlowLog()))
                  .addAllKeys(codec.encodeKeys(keys))
                  .build();
      RegionErrorHandler<RawBatchGetResponse> handler =
          new RegionErrorHandler<RawBatchGetResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawBatchGetResponse resp =
          callWithRetry(backoffer, TikvGrpc.getRawBatchGetMethod(), factory, handler);
      return handleRawBatchGet(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private List<KvPair> handleRawBatchGet(RawBatchGetResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawBatchPutResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }

    return codec.decodeKvPairs(resp.getPairsList());
  }

  public void rawBatchPut(
      BackOffer backOffer, List<KvPair> kvPairs, long ttl, boolean atomicForCAS) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_batch_put", clusterId.toString())
            .startTimer();
    try {
      if (kvPairs.isEmpty()) {
        return;
      }
      Supplier<RawBatchPutRequest> factory =
          () ->
              RawBatchPutRequest.newBuilder()
                  .setContext(makeContext(storeType, backOffer.getSlowLog()))
                  .addAllPairs(kvPairs)
                  .setTtl(ttl)
                  .addTtls(ttl)
                  .setForCas(atomicForCAS)
                  .build();
      RegionErrorHandler<RawBatchPutResponse> handler =
          new RegionErrorHandler<RawBatchPutResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawBatchPutResponse resp =
          callWithRetry(backOffer, TikvGrpc.getRawBatchPutMethod(), factory, handler);
      handleRawBatchPut(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  public void rawBatchPut(BackOffer backOffer, Batch batch, long ttl, boolean atomicForCAS) {
    List<KvPair> pairs = new ArrayList<>();
    for (int i = 0; i < batch.getKeys().size(); i++) {
      pairs.add(
          KvPair.newBuilder()
              .setKey(codec.encodeKey(batch.getKeys().get(i)))
              .setValue(batch.getValues().get(i))
              .build());
    }
    rawBatchPut(backOffer, pairs, ttl, atomicForCAS);
  }

  private void handleRawBatchPut(RawBatchPutResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawBatchPutResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
  }

  public void rawBatchDelete(BackOffer backoffer, List<ByteString> keys, boolean atomicForCAS) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_batch_delete", clusterId.toString())
            .startTimer();
    try {
      if (keys.isEmpty()) {
        return;
      }
      Supplier<RawBatchDeleteRequest> factory =
          () ->
              RawBatchDeleteRequest.newBuilder()
                  .setContext(makeContext(storeType, backoffer.getSlowLog()))
                  .addAllKeys(codec.encodeKeys(keys))
                  .setForCas(atomicForCAS)
                  .build();
      RegionErrorHandler<RawBatchDeleteResponse> handler =
          new RegionErrorHandler<RawBatchDeleteResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawBatchDeleteResponse resp =
          callWithRetry(backoffer, TikvGrpc.getRawBatchDeleteMethod(), factory, handler);
      handleRawBatchDelete(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private void handleRawBatchDelete(RawBatchDeleteResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawBatchDeleteResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
  }

  /**
   * Return a batch KvPair list containing limited key-value pairs starting from `key`, which are in
   * the same region
   *
   * @param backOffer BackOffer
   * @param key startKey
   * @param keyOnly true if value of KvPair is not needed
   * @return KvPair list
   */
  public List<KvPair> rawScan(BackOffer backOffer, ByteString key, int limit, boolean keyOnly) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_scan", clusterId.toString()).startTimer();
    try {
      Supplier<RawScanRequest> factory =
          () -> {
            Pair<ByteString, ByteString> range = codec.encodeRange(key, ByteString.EMPTY);
            return RawScanRequest.newBuilder()
                .setContext(makeContext(storeType, backOffer.getSlowLog()))
                .setStartKey(range.first)
                .setEndKey(range.second)
                .setKeyOnly(keyOnly)
                .setLimit(limit)
                .build();
          };

      RegionErrorHandler<RawScanResponse> handler =
          new RegionErrorHandler<RawScanResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawScanResponse resp =
          callWithRetry(backOffer, TikvGrpc.getRawScanMethod(), factory, handler);
      // RegionErrorHandler may refresh region cache due to outdated region info,
      // This region need to get newest info from cache.
      region = regionManager.getRegionByKey(key, backOffer);
      return rawScanHelper(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  public List<KvPair> rawScan(BackOffer backOffer, ByteString key, boolean keyOnly) {
    return rawScan(backOffer, key, getConf().getScanBatchSize(), keyOnly);
  }

  private List<KvPair> rawScanHelper(RawScanResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawScanResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return codec.decodeKvPairs(resp.getKvsList());
  }

  /**
   * Delete raw keys in the range of [startKey, endKey)
   *
   * @param backOffer BackOffer
   * @param startKey startKey
   * @param endKey endKey
   */
  public void rawDeleteRange(BackOffer backOffer, ByteString startKey, ByteString endKey) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_delete_range", clusterId.toString())
            .startTimer();
    try {
      Supplier<RawDeleteRangeRequest> factory =
          () -> {
            Pair<ByteString, ByteString> range = codec.encodeRange(startKey, endKey);
            return RawDeleteRangeRequest.newBuilder()
                .setContext(makeContext(storeType, backOffer.getSlowLog()))
                .setStartKey(range.first)
                .setEndKey(range.second)
                .build();
          };

      RegionErrorHandler<RawDeleteRangeResponse> handler =
          new RegionErrorHandler<RawDeleteRangeResponse>(
              regionManager, this, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawDeleteRangeResponse resp =
          callWithRetry(backOffer, TikvGrpc.getRawDeleteRangeMethod(), factory, handler);
      rawDeleteRangeHelper(resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  private void rawDeleteRangeHelper(RawDeleteRangeResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawDeleteRangeResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
  }

  public enum RequestTypes {
    REQ_TYPE_SELECT(101),
    REQ_TYPE_INDEX(102),
    REQ_TYPE_DAG(103),
    REQ_TYPE_ANALYZE(104),
    BATCH_ROW_COUNT(64);

    private final int value;

    RequestTypes(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  public static class RegionStoreClientBuilder {

    private final TiConfiguration conf;
    private final ChannelFactory channelFactory;
    private final RegionManager regionManager;
    private final PDClient pdClient;

    public RegionStoreClientBuilder(
        TiConfiguration conf,
        ChannelFactory channelFactory,
        RegionManager regionManager,
        PDClient pdClient) {
      Objects.requireNonNull(conf, "conf is null");
      Objects.requireNonNull(channelFactory, "channelFactory is null");
      Objects.requireNonNull(regionManager, "regionManager is null");
      this.conf = conf;
      this.channelFactory = channelFactory;
      this.regionManager = regionManager;
      this.pdClient = pdClient;
    }

    public RegionStoreClient build(TiRegion region, TiStore store, TiStoreType storeType)
        throws GrpcException {
      Objects.requireNonNull(region, "region is null");
      Objects.requireNonNull(store, "store is null");
      Objects.requireNonNull(storeType, "storeType is null");

      String addressStr = store.getStore().getAddress();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Create region store client on address %s", addressStr));
      }
      ManagedChannel channel = null;

      TikvBlockingStub blockingStub = null;
      TikvFutureStub asyncStub = null;

      if (conf.getEnableGrpcForward() && store.getProxyStore() != null && !store.isReachable()) {
        addressStr = store.getProxyStore().getAddress();
        channel =
            channelFactory.getChannel(addressStr, regionManager.getPDClient().getHostMapping());
        Metadata header = new Metadata();
        header.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
        blockingStub = MetadataUtils.attachHeaders(TikvGrpc.newBlockingStub(channel), header);
        asyncStub = MetadataUtils.attachHeaders(TikvGrpc.newFutureStub(channel), header);
      } else {
        channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
        blockingStub = TikvGrpc.newBlockingStub(channel);
        asyncStub = TikvGrpc.newFutureStub(channel);
      }

      return new RegionStoreClient(
          conf,
          region,
          store,
          storeType,
          channelFactory,
          blockingStub,
          asyncStub,
          regionManager,
          pdClient,
          this);
    }

    public synchronized RegionStoreClient build(TiRegion region, TiStore store)
        throws GrpcException {
      return build(region, store, TiStoreType.TiKV);
    }

    public synchronized RegionStoreClient build(ByteString key) throws GrpcException {
      return build(key, TiStoreType.TiKV);
    }

    public synchronized RegionStoreClient build(ByteString key, BackOffer backOffer)
        throws GrpcException {
      return build(key, TiStoreType.TiKV, backOffer);
    }

    public synchronized RegionStoreClient build(ByteString key, TiStoreType storeType)
        throws GrpcException {
      return build(key, storeType, defaultBackOff());
    }

    public synchronized RegionStoreClient build(
        ByteString key, TiStoreType storeType, BackOffer backOffer) throws GrpcException {
      Pair<TiRegion, TiStore> pair =
          regionManager.getRegionStorePairByKey(key, storeType, backOffer);
      return build(pair.first, pair.second, storeType);
    }

    public synchronized RegionStoreClient build(TiRegion region) throws GrpcException {
      return build(region, defaultBackOff());
    }

    public synchronized RegionStoreClient build(TiRegion region, BackOffer backOffer)
        throws GrpcException {
      TiStore store = regionManager.getStoreById(region.getLeader().getStoreId(), backOffer);
      return build(region, store, TiStoreType.TiKV);
    }

    public RegionManager getRegionManager() {
      return regionManager;
    }

    private BackOffer defaultBackOff() {
      BackOffer backoffer =
          ConcreteBackOffer.newCustomBackOff(
              conf.getRawKVDefaultBackoffInMS(), pdClient.getClusterId());
      return backoffer;
    }
  }
}
