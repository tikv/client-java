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

import static org.tikv.common.util.BackOffFunction.BackOffFuncType.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.util.*;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.*;
import org.tikv.common.operation.KVErrorHandler;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb.*;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;
import org.tikv.txn.AbstractLockResolverClient;
import org.tikv.txn.Lock;
import org.tikv.txn.ResolveLockResult;

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
  /** startTS -> List(locks) */
  private final Map<Long, Set<Long>> resolvedLocks = new HashMap<>();

  private RegionStoreClient(
      TiConfiguration conf,
      TiRegion region,
      Store store,
      ChannelFactory channelFactory,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub,
      RegionManager regionManager,
      PDClient pdClient,
      RegionStoreClient.RegionStoreClientBuilder clientBuilder) {
    super(conf, region, channelFactory, blockingStub, asyncStub, regionManager);

    this.lockResolverClient =
        AbstractLockResolverClient.getInstance(
            store,
            conf,
            region,
            this.blockingStub,
            this.asyncStub,
            channelFactory,
            regionManager,
            pdClient,
            clientBuilder);
  }

  public synchronized boolean addResolvedLocks(Long version, List<Long> locks) {
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
                .setContext(region.getContext(getResolvedLocks(version)))
                .setKey(key)
                .setVersion(version)
                .build();

    KVErrorHandler<GetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
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

  public List<KvPair> batchGet(BackOffer backOffer, Iterable<ByteString> keys, long version) {
    boolean forWrite = false;
    Supplier<BatchGetRequest> request =
        () ->
            BatchGetRequest.newBuilder()
                .setContext(region.getContext(getResolvedLocks(version)))
                .addAllKeys(keys)
                .setVersion(version)
                .build();
    KVErrorHandler<BatchGetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
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
          Lock lock = new Lock(pair.getError().getLocked());
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
    } else {
      return resp.getPairsList();
    }
  }

  public List<KvPair> scan(
      BackOffer backOffer, ByteString startKey, long version, boolean keyOnly) {
    boolean forWrite = false;
    while (true) {
      // we should refresh region
      region = regionManager.getRegionByKey(startKey);

      Supplier<ScanRequest> request =
          () ->
              ScanRequest.newBuilder()
                  .setContext(region.getContext(getResolvedLocks(version)))
                  .setStartKey(startKey)
                  .setVersion(version)
                  .setKeyOnly(keyOnly)
                  .setLimit(getConf().getScanBatchSize())
                  .build();

      KVErrorHandler<ScanResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null,
              resolveLockResult -> addResolvedLocks(version, resolveLockResult.getResolvedLocks()),
              version,
              forWrite);
      ScanResponse resp = callWithRetry(backOffer, TikvGrpc.getKvScanMethod(), request, handler);
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
        Lock lock = AbstractLockResolverClient.extractLockFromKeyErr(kvPair.getError());
        newKvPairs.add(
            KvPair.newBuilder()
                .setError(kvPair.getError())
                .setValue(kvPair.getValue())
                .setKey(lock.getKey())
                .build());
      } else {
        newKvPairs.add(kvPair);
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
      BackOffer backOffer,
      ByteString primary,
      Iterable<Mutation> mutations,
      long startTs,
      long lockTTL)
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
      Iterable<Mutation> mutations,
      long startTs,
      long ttl,
      boolean skipConstraintCheck)
      throws TiClientInternalException, KeyException, RegionException {
    boolean forWrite = true;
    while (true) {
      Supplier<PrewriteRequest> factory =
          () ->
              PrewriteRequest.newBuilder()
                  .setContext(region.getContext())
                  .setStartVersion(startTs)
                  .setPrimaryLock(primaryLock)
                  .addAllMutations(mutations)
                  .setLockTtl(ttl)
                  .setSkipConstraintCheck(skipConstraintCheck)
                  .setMinCommitTs(startTs)
                  .setTxnSize(16)
                  .build();
      KVErrorHandler<PrewriteResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              region,
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
        Lock lock = new Lock(err.getLocked());
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
                  .setContext(region.getContext())
                  .setStartVersion(startTs)
                  .setPrimaryLock(primaryLock)
                  .setAdviseLockTtl(ttl)
                  .build();
      KVErrorHandler<TxnHeartBeatResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              region,
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
                .addAllKeys(keys)
                .setContext(region.getContext())
                .build();
    KVErrorHandler<CommitResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
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

  // APIs for Raw Scan/Put/Get/Delete

  public ByteString rawGet(BackOffer backOffer, ByteString key) {
    Supplier<RawGetRequest> factory =
        () -> RawGetRequest.newBuilder().setContext(region.getContext()).setKey(key).build();
    KVErrorHandler<RawGetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawGetResponse resp = callWithRetry(backOffer, TikvGrpc.getRawGetMethod(), factory, handler);
    return rawGetHelper(resp);
  }

  private ByteString rawGetHelper(RawGetResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawGetResponse failed without a cause");
    }
    String error = resp.getError();
    if (error != null && !error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getValue();
  }

  public void rawDelete(BackOffer backOffer, ByteString key) {
    Supplier<RawDeleteRequest> factory =
        () -> RawDeleteRequest.newBuilder().setContext(region.getContext()).setKey(key).build();

    KVErrorHandler<RawDeleteResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawDeleteResponse resp =
        callWithRetry(backOffer, TikvGrpc.getRawDeleteMethod(), factory, handler);
    rawDeleteHelper(resp, region);
  }

  private void rawDeleteHelper(RawDeleteResponse resp, TiRegion region) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawDeleteResponse failed without a cause");
    }
    String error = resp.getError();
    if (error != null && !error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
  }

  public void rawPut(BackOffer backOffer, ByteString key, ByteString value) {
    Supplier<RawPutRequest> factory =
        () ->
            RawPutRequest.newBuilder()
                .setContext(region.getContext())
                .setKey(key)
                .setValue(value)
                .build();

    KVErrorHandler<RawPutResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawPutResponse resp = callWithRetry(backOffer, TikvGrpc.getRawPutMethod(), factory, handler);
    rawPutHelper(resp);
  }

  private void rawPutHelper(RawPutResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawPutResponse failed without a cause");
    }
    String error = resp.getError();
    if (error != null && !error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
  }

  public void rawBatchPut(BackOffer backOffer, List<KvPair> kvPairs) {
    if (kvPairs.isEmpty()) {
      return;
    }
    Supplier<RawBatchPutRequest> factory =
        () ->
            RawBatchPutRequest.newBuilder()
                .setContext(region.getContext())
                .addAllPairs(kvPairs)
                .build();
    KVErrorHandler<RawBatchPutResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawBatchPutResponse resp =
        callWithRetry(backOffer, TikvGrpc.getRawBatchPutMethod(), factory, handler);
    handleRawBatchPut(resp);
  }

  private void handleRawBatchPut(RawBatchPutResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawBatchPutResponse failed without a cause");
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
  private List<KvPair> rawScan(BackOffer backOffer, ByteString key, int limit, boolean keyOnly) {
    Supplier<RawScanRequest> factory =
        () ->
            RawScanRequest.newBuilder()
                .setContext(region.getContext())
                .setStartKey(key)
                .setKeyOnly(keyOnly)
                .setLimit(limit)
                .build();

    KVErrorHandler<RawScanResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawScanResponse resp = callWithRetry(backOffer, TikvGrpc.getRawScanMethod(), factory, handler);
    return rawScanHelper(resp);
  }

  public List<KvPair> rawScan(BackOffer backOffer, ByteString key) {
    return rawScan(backOffer, key, getConf().getScanBatchSize());
  }

  public List<KvPair> rawScan(BackOffer backOffer, ByteString key, int limit) {
    return rawScan(backOffer, key, limit, false);
  }

  private List<KvPair> rawScanHelper(RawScanResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("RawScanResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getKvsList();
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

    public RegionStoreClient build(TiRegion region, Store store) throws GrpcException {
      Objects.requireNonNull(region, "region is null");
      Objects.requireNonNull(store, "store is null");

      String addressStr = store.getAddress();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Create region store client on address %s", addressStr));
      }
      ManagedChannel channel = channelFactory.getChannel(addressStr);

      TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);
      TikvStub asyncStub = TikvGrpc.newStub(channel);

      return new RegionStoreClient(
          conf,
          region,
          store,
          channelFactory,
          blockingStub,
          asyncStub,
          regionManager,
          pdClient,
          this);
    }

    public RegionStoreClient build(ByteString key) {
      Pair<TiRegion, Store> pair = regionManager.getRegionStorePairByKey(key);
      return build(pair.first, pair.second);
    }

    public RegionStoreClient build(TiRegion region) throws GrpcException {
      Store store = regionManager.getStoreById(region.getLeader().getStoreId());
      return build(region, store);
    }

    public RegionManager getRegionManager() {
      return regionManager;
    }
  }
}
