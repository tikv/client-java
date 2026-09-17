/*
 * Copyright 2026 TiKV Project Authors.
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

package org.tikv.raw;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.exception.KeyException;
import org.tikv.common.exception.RegionException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Metapb.Peer;

/**
 * An asynchronous RawKV client based on the gRPC FutureStub.
 *
 * <p>As the counterpart of the blocking {@link RawKVClient}, it covers RawKV single-key CRUD
 * operations (get/put/delete/getKeyTTL), CAS (putIfAbsent/compareAndSet), scan and deleteRange,
 * without Txn, SQL coprocessor or batch support. All methods return {@link CompletableFuture};
 * network IO is fully asynchronous and never blocks the calling thread.
 *
 * <p>Implementation notes:
 *
 * <ul>
 *   <li>The metadata executor can be supplied by the caller; its lifecycle then stays with the
 *       caller and is not touched by {@link #close()}.
 *   <li>Metadata lookup is two-stage: the calling thread first checks the region/store caches; only
 *       on a cache miss the (possibly blocking) PD lookup runs on the dedicated metadata executor,
 *       and the result is chained back asynchronously.
 *   <li>Every method sends exactly one asynchronous gRPC request per target Region and returns a
 *       {@link CompletableFuture} wrapping either the decoded result or an exception. No retry is
 *       performed: retry policy is entirely controlled by the caller.
 *   <li>On {@code RegionError}, the cached Region metadata is invalidated ({@link
 *       RegionManager#onRequestFail}) and the future fails with {@link RegionException}; the caller
 *       decides whether to retry.
 *   <li>Scan and deleteRange are single-shot requests against one Region: the caller is responsible
 *       for iterating across Region boundaries.
 * </ul>
 */
public class RawKVAsyncClient implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(RawKVAsyncClient.class);
  private static final int DEFAULT_METADATA_POOL_SIZE = 4;

  private final TiConfiguration conf;
  private final RegionStoreClientBuilder clientBuilder;
  private final RegionManager regionManager;
  private final RequestKeyCodec codec;
  private final boolean atomicForCAS;
  private final long clusterId;

  // Executor used to run the (possibly PD-blocking) Region/Store lookup on cache miss.
  private final ExecutorService metadataExecutor;
  private final boolean shutdownExecutorOnClose;

  /**
   * Creates a client with an internally managed metadata executor: a fixed thread pool of {@link
   * #DEFAULT_METADATA_POOL_SIZE} daemon threads, shut down automatically on {@link #close()}.
   */
  public RawKVAsyncClient(TiSession session, RegionStoreClientBuilder clientBuilder) {
    this(session, clientBuilder, DEFAULT_METADATA_POOL_SIZE);
  }

  /**
   * Creates a client with an internally managed metadata executor: a fixed thread pool of daemon
   * threads, shut down automatically on {@link #close()}.
   */
  public RawKVAsyncClient(TiSession session, RegionStoreClientBuilder clientBuilder, int threads) {
    this(
        session,
        clientBuilder,
        Executors.newFixedThreadPool(
            threads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("raw-async-metadata-%d")
                .build()),
        true);
  }

  /**
   * Creates a client with a caller-supplied metadata executor. The executor's lifecycle stays with
   * the caller: {@link #close()} does not shut it down.
   */
  public RawKVAsyncClient(
      TiSession session, RegionStoreClientBuilder clientBuilder, ExecutorService metadataExecutor) {
    this(session, clientBuilder, metadataExecutor, false);
  }

  private RawKVAsyncClient(
      TiSession session,
      RegionStoreClientBuilder clientBuilder,
      ExecutorService metadataExecutor,
      boolean shutdownExecutorOnClose) {
    Objects.requireNonNull(session, "session is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    Objects.requireNonNull(metadataExecutor, "metadataExecutor is null");
    this.conf = session.getConf();
    this.clientBuilder = clientBuilder;
    this.regionManager = clientBuilder.getRegionManager();
    this.codec = regionManager.getPDClient().getCodec();
    this.atomicForCAS = conf.isEnableAtomicForCAS();
    this.clusterId = session.getPDClient().getClusterId();
    this.metadataExecutor = metadataExecutor;
    this.shutdownExecutorOnClose = shutdownExecutorOnClose;
    logger.info("create async client, clusterId: {}", clusterId);
  }

  /**
   * Closes the client. The metadata executor is only shut down here when it was created internally;
   * a caller-supplied executor is left running and must be shut down by the caller.
   */
  @Override
  public void close() {
    if (shutdownExecutorOnClose) {
      metadataExecutor.shutdown();
    }
  }

  /**
   * Async get.
   *
   * @return Optional.empty means the key does not exist
   */
  public CompletableFuture<Optional<ByteString>> getAsync(ByteString key) {
    return buildClientAsync(key)
        .thenCompose(
            client ->
                sendAsync(
                    client,
                    c -> c.rawGetAsync(key),
                    resp -> resp.hasRegionError() ? resp.getRegionError() : null))
        .thenApply(
            resp -> {
              if (!resp.getError().isEmpty()) {
                throw new KeyException(resp.getError());
              }
              if (resp.getNotFound()) {
                return Optional.empty();
              }
              return Optional.of(resp.getValue());
            });
  }

  /** Async put. */
  public CompletableFuture<Void> putAsync(ByteString key, ByteString value) {
    return putAsync(key, value, 0L);
  }

  /** Async put. ttl is in seconds; 0 means never expire. */
  public CompletableFuture<Void> putAsync(ByteString key, ByteString value, long ttl) {
    return buildClientAsync(key)
        .thenCompose(
            client ->
                sendAsync(
                    client,
                    c -> c.rawPutAsync(key, value, ttl, atomicForCAS),
                    resp -> resp.hasRegionError() ? resp.getRegionError() : null))
        .thenApply(
            resp -> {
              if (!resp.getError().isEmpty()) {
                throw new KeyException(resp.getError());
              }
              return null;
            });
  }

  /** Async delete. */
  public CompletableFuture<Void> deleteAsync(ByteString key) {
    return buildClientAsync(key)
        .thenCompose(
            client ->
                sendAsync(
                    client,
                    c -> c.rawDeleteAsync(key, atomicForCAS),
                    resp -> resp.hasRegionError() ? resp.getRegionError() : null))
        .thenApply(
            resp -> {
              if (!resp.getError().isEmpty()) {
                throw new KeyException(resp.getError());
              }
              return null;
            });
  }

  /** Async getKeyTTL. Returns Optional.empty when the key does not exist. */
  public CompletableFuture<Optional<Long>> getKeyTTLAsync(ByteString key) {
    return buildClientAsync(key)
        .thenCompose(
            client ->
                sendAsync(
                    client,
                    c -> c.rawGetKeyTTLAsync(key),
                    resp -> resp.hasRegionError() ? resp.getRegionError() : null))
        .thenApply(
            resp -> {
              if (!resp.getError().isEmpty()) {
                throw new KeyException(resp.getError());
              }
              if (resp.getNotFound()) {
                return Optional.empty();
              }
              return Optional.of(resp.getTtl());
            });
  }

  /** Async putIfAbsent. Returns Optional.empty on success, otherwise the previous value. */
  public CompletableFuture<Optional<ByteString>> putIfAbsentAsync(
      ByteString key, ByteString value) {
    return putIfAbsentAsync(key, value, 0L);
  }

  /** Async putIfAbsent with ttl. */
  public CompletableFuture<Optional<ByteString>> putIfAbsentAsync(
      ByteString key, ByteString value, long ttl) {
    return compareAndSetAsync(key, Optional.empty(), value, ttl)
        .thenApply(pair -> pair.first ? Optional.empty() : pair.second);
  }

  /**
   * Async compareAndSet. The result Pair carries the outcome: {@code first} is true when the CAS
   * succeeded, false otherwise; on failure, {@code second} is the current value, or {@code
   * Optional.empty()} when the key does not exist.
   */
  public CompletableFuture<Pair<Boolean, Optional<ByteString>>> compareAndSetAsync(
      ByteString key, Optional<ByteString> prevValue, ByteString value) {
    return compareAndSetAsync(key, prevValue, value, 0L);
  }

  /** Async compareAndSet with ttl. */
  public CompletableFuture<Pair<Boolean, Optional<ByteString>>> compareAndSetAsync(
      ByteString key, Optional<ByteString> prevValue, ByteString value, long ttl) {
    if (!atomicForCAS) {
      return failedFuture(
          new IllegalArgumentException(
              "To use compareAndSet or putIfAbsent, please enable the config tikv.enable_atomic_for_cas."));
    }
    return buildClientAsync(key)
        .thenCompose(
            client ->
                sendAsync(
                    client,
                    c -> c.rawCompareAndSetAsync(key, prevValue, value, ttl),
                    resp -> resp.hasRegionError() ? resp.getRegionError() : null))
        .thenApply(
            resp -> {
              if (!resp.getError().isEmpty()) {
                throw new KeyException(resp.getError());
              }
              if (resp.getSucceed()) {
                return Pair.create(true, Optional.empty());
              }
              return resp.getPreviousNotExist()
                  ? Pair.create(false, Optional.empty())
                  : Pair.create(false, Optional.of(resp.getPreviousValue()));
            });
  }

  /**
   * Async scan from startKey within a single Region, at most limit pairs.
   *
   * <p>This is a single-shot request: it does not continue across Region boundaries. The caller
   * controls the scan range and is responsible for iterating Regions.
   */
  public CompletableFuture<List<KvPair>> scanAsync(ByteString startKey, int limit) {
    return scanAsync(startKey, limit, false);
  }

  /** Async scan with keyOnly support. */
  public CompletableFuture<List<KvPair>> scanAsync(
      ByteString startKey, int limit, boolean keyOnly) {
    if (limit <= 0 || limit > RawKVClientBase.MAX_RAW_SCAN_LIMIT) {
      return failedFuture(
          new TiKVException("limit should be in (0, MAX_RAW_SCAN_LIMIT]: " + limit));
    }
    return buildClientAsync(startKey)
        .thenCompose(
            client ->
                sendAsync(
                    client,
                    c -> c.rawScanAsync(startKey, limit, keyOnly),
                    resp -> resp.hasRegionError() ? resp.getRegionError() : null))
        .thenApply(resp -> codec.decodeKvPairs(resp.getKvsList()));
  }

  /**
   * Async deleteRange over [startKey, endKey).
   *
   * <p>This is a single-shot request against one Region: if the range crosses Region boundaries the
   * server returns a RegionError, and splitting the range is left to the caller.
   */
  public CompletableFuture<Void> deleteRangeAsync(ByteString startKey, ByteString endKey) {
    return buildClientAsync(startKey)
        .thenCompose(
            client ->
                sendAsync(
                    client,
                    c -> c.rawDeleteRangeAsync(startKey, endKey),
                    resp -> resp.hasRegionError() ? resp.getRegionError() : null))
        .thenApply(
            resp -> {
              if (!resp.getError().isEmpty()) {
                throw new KeyException(resp.getError());
              }
              return null;
            });
  }

  /**
   * Sends one asynchronous gRPC request and translates failures into the future.
   *
   * <ul>
   *   <li>gRPC transport error -> {@link TiKVException}
   *   <li>RegionError -> invalidate Region cache + {@link RegionException}
   *   <li>otherwise -> the raw protobuf response for the caller to decode
   * </ul>
   *
   * No retry is performed here; the caller decides what to do with the failure.
   */
  private <T> CompletableFuture<T> sendAsync(
      RegionStoreClient client,
      Function<RegionStoreClient, ListenableFuture<T>> rpcAsyncFn,
      Function<T, Errorpb.Error> checkRegionError) {
    CompletableFuture<T> result = new CompletableFuture<>();
    final ListenableFuture<T> future;
    try {
      future = rpcAsyncFn.apply(client);
    } catch (Exception e) {
      result.completeExceptionally(e);
      return result;
    }
    future.addListener(
        () -> {
          final T resp;
          try {
            resp = future.get();
          } catch (Exception e) {
            // gRPC transport error
            result.completeExceptionally(new TiKVException("grpc error", e));
            return;
          }
          Errorpb.Error regionError = checkRegionError.apply(resp);
          if (regionError != null) {
            // Stale region metadata: invalidate the cache; retry is up to the caller.
            regionManager.onRequestFail(client.getRegion());
            result.completeExceptionally(new RegionException(regionError));
            return;
          }
          result.complete(resp);
        },
        MoreExecutors.directExecutor());
    return result;
  }

  // ==================== Metadata lookup ====================

  /**
   * Builds a client for the key's Region without blocking the calling thread.
   *
   * <p>Fast path: the Region and its leader Store are served from the caches. Slow path: the
   * (possibly PD-blocking) lookup runs on the metadata executor.
   */
  private CompletableFuture<RegionStoreClient> buildClientAsync(ByteString key) {
    TiRegion cachedRegion = regionManager.getRegionByKeyFromCache(key);
    if (cachedRegion != null) {
      return buildClientForRegionAsync(cachedRegion);
    }
    return submitOnMetadataExecutor(() -> clientBuilder.build(key));
  }

  /**
   * Builds a client for a known Region without blocking the calling thread.
   *
   * <p>Fast path: the leader Store is served from the cache. Slow path: the lookup runs on the
   * metadata executor.
   */
  private CompletableFuture<RegionStoreClient> buildClientForRegionAsync(TiRegion region) {
    Peer leader = region.getLeader();
    if (leader != null) {
      TiStore cachedStore = regionManager.getStoreByIdFromCache(leader.getStoreId());
      if (cachedStore != null) {
        try {
          return CompletableFuture.completedFuture(clientBuilder.build(region, cachedStore));
        } catch (Exception e) {
          return failedFuture(e);
        }
      }
    }
    return submitOnMetadataExecutor(() -> clientBuilder.build(region));
  }

  /**
   * Submits a task to the metadata executor.
   *
   * <p>If the executor rejects the task (e.g. it is shut down or its task queue is full) or the
   * task itself fails, the failure is captured in the returned future.
   */
  private <T> CompletableFuture<T> submitOnMetadataExecutor(Supplier<T> supplier) {
    try {
      return CompletableFuture.supplyAsync(supplier, metadataExecutor);
    } catch (RejectedExecutionException e) {
      return failedFuture(e);
    }
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable e) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(e);
    return f;
  }
}
