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

package org.tikv.txn;

import static org.tikv.common.util.ClientUtils.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.importer.ImporterClient;
import org.tikv.common.importer.SwitchTiKVModeClient;
import org.tikv.common.key.Key;
import org.tikv.common.operation.iterator.ConcreteScanIterator;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.*;
import org.tikv.kvproto.Kvrpcpb;

public class KVClient implements AutoCloseable {
  private final TiSession tiSession;
  private static final Logger logger = LoggerFactory.getLogger(KVClient.class);
  private static final int MAX_BATCH_LIMIT = 1024;
  private static final int BATCH_GET_SIZE = 16 * 1024;
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final ExecutorService executorService;
  private Set<Long> resolvedLocks = Collections.emptySet();

  public KVClient(TiConfiguration conf, RegionStoreClientBuilder clientBuilder, TiSession session) {
    Objects.requireNonNull(conf, "conf is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.tiSession = session;
    this.conf = conf;
    this.clientBuilder = clientBuilder;
    executorService =
        Executors.newFixedThreadPool(
            conf.getKvClientConcurrency(),
            new ThreadFactoryBuilder().setNameFormat("kvclient-pool-%d").setDaemon(true).build());
  }

  @Override
  public void close() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  /**
   * Get a key-value pair from TiKV if key exists
   *
   * @param key key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString get(ByteString key, long version) throws GrpcException {
    BackOffer backOffer =
        ConcreteBackOffer.newGetBackOff(
            clientBuilder.getRegionManager().getPDClient().getClusterId());
    while (true) {
      RegionStoreClient client = clientBuilder.build(key);
      try {
        return client.get(backOffer, key, version);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
  }

  /**
   * Get a set of key-value pair by keys from TiKV
   *
   * @param backOffer
   * @param keys
   * @param version
   * @return
   * @throws GrpcException
   */
  public List<Kvrpcpb.KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version)
      throws GrpcException {
    return doSendBatchGet(backOffer, keys, version);
  }

  /**
   * Scan key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey start key, inclusive
   * @param endKey end key, exclusive
   * @return list of key-value pairs in range
   */
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, long version)
      throws GrpcException {
    Iterator<Kvrpcpb.KvPair> iterator =
        scanIterator(conf, clientBuilder, startKey, endKey, version);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  /**
   * Scan key-value pairs from TiKV in range [startKey, ♾), maximum to `limit` pairs
   *
   * @param startKey start key, inclusive
   * @param limit limit of kv pairs
   * @return list of key-value pairs in range
   */
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, long version, int limit)
      throws GrpcException {
    Iterator<Kvrpcpb.KvPair> iterator = scanIterator(conf, clientBuilder, startKey, version, limit);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  public List<Kvrpcpb.KvPair> scan(ByteString startKey, long version) throws GrpcException {
    return scan(startKey, version, Integer.MAX_VALUE);
  }

  public synchronized void ingest(List<Pair<ByteString, ByteString>> list) throws GrpcException {
    if (list.isEmpty()) {
      return;
    }

    Key min = Key.MAX;
    Key max = Key.MIN;
    Map<ByteString, ByteString> map = new HashMap<>(list.size());

    for (Pair<ByteString, ByteString> pair : list) {
      map.put(pair.first, pair.second);
      Key key = Key.toRawKey(pair.first.toByteArray());
      if (key.compareTo(min) < 0) {
        min = key;
      }
      if (key.compareTo(max) > 0) {
        max = key;
      }
    }

    SwitchTiKVModeClient switchTiKVModeClient = tiSession.getSwitchTiKVModeClient();

    try {
      // switch to normal mode
      switchTiKVModeClient.switchTiKVToNormalMode();

      // region split
      List<byte[]> splitKeys = new ArrayList<>(2);
      splitKeys.add(min.getBytes());
      splitKeys.add(max.next().getBytes());

      tiSession.splitRegionAndScatter(splitKeys);
      tiSession.getRegionManager().invalidateAll();

      // switch to import mode
      switchTiKVModeClient.keepTiKVToImportMode();

      // group keys by region
      List<ByteString> keyList = list.stream().map(pair -> pair.first).collect(Collectors.toList());
      Map<TiRegion, List<ByteString>> groupKeys =
          groupKeysByRegion(
              clientBuilder.getRegionManager(),
              keyList,
              ConcreteBackOffer.newRawKVBackOff(tiSession.getPDClient().getClusterId()));

      // ingest for each region
      for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
        TiRegion region = entry.getKey();
        List<ByteString> keys = entry.getValue();
        List<Pair<ByteString, ByteString>> kvs =
            keys.stream().map(k -> Pair.create(k, map.get(k))).collect(Collectors.toList());
        doIngest(region, kvs);
      }
    } finally {
      // swith tikv to normal mode
      switchTiKVModeClient.stopKeepTiKVToImportMode();
      switchTiKVModeClient.switchTiKVToNormalMode();
    }
  }

  private List<Kvrpcpb.KvPair> doSendBatchGet(
      BackOffer backOffer, List<ByteString> keys, long version) {
    ExecutorCompletionService<List<Kvrpcpb.KvPair>> completionService =
        new ExecutorCompletionService<>(executorService);

    List<Batch> batches =
        getBatches(backOffer, keys, BATCH_GET_SIZE, MAX_BATCH_LIMIT, this.clientBuilder);

    for (Batch batch : batches) {
      completionService.submit(
          () -> doSendBatchGetInBatchesWithRetry(batch.getBackOffer(), batch, version));
    }

    return getKvPairs(completionService, batches, BackOffer.BATCH_GET_MAX_BACKOFF);
  }

  private List<Kvrpcpb.KvPair> doSendBatchGetInBatchesWithRetry(
      BackOffer backOffer, Batch batch, long version) {
    TiRegion oldRegion = batch.getRegion();
    TiRegion currentRegion =
        clientBuilder.getRegionManager().getRegionByKey(oldRegion.getStartKey());

    if (oldRegion.equals(currentRegion)) {
      RegionStoreClient client = clientBuilder.build(batch.getRegion());
      // set resolvedLocks for the new client
      if (!resolvedLocks.isEmpty()) {
        client.addResolvedLocks(version, resolvedLocks);
      }
      try {
        return client.batchGet(backOffer, batch.getKeys(), version);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        clientBuilder.getRegionManager().invalidateRegion(batch.getRegion());
        logger.warn("ReSplitting ranges for BatchGetRequest", e);

        // get resolved locks and retry
        resolvedLocks = client.getResolvedLocks(version);
        return doSendBatchGetWithRefetchRegion(backOffer, batch, version);
      }
    } else {
      return doSendBatchGetWithRefetchRegion(backOffer, batch, version);
    }
  }

  private List<Kvrpcpb.KvPair> doSendBatchGetWithRefetchRegion(
      BackOffer backOffer, Batch batch, long version) {
    List<Batch> retryBatches =
        getBatches(backOffer, batch.getKeys(), BATCH_GET_SIZE, MAX_BATCH_LIMIT, this.clientBuilder);

    ArrayList<Kvrpcpb.KvPair> results = new ArrayList<>();
    for (Batch retryBatch : retryBatches) {
      // recursive calls
      List<Kvrpcpb.KvPair> batchResult =
          doSendBatchGetInBatchesWithRetry(backOffer, retryBatch, version);
      results.addAll(batchResult);
    }
    return results;
  }

  private Iterator<Kvrpcpb.KvPair> scanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    return new ConcreteScanIterator(conf, builder, startKey, endKey, version);
  }

  private Iterator<Kvrpcpb.KvPair> scanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      long version,
      int limit) {
    return new ConcreteScanIterator(conf, builder, startKey, version, limit);
  }

  private void doIngest(TiRegion region, List<Pair<ByteString, ByteString>> sortedList)
      throws GrpcException {
    if (sortedList.isEmpty()) {
      return;
    }

    ByteString uuid = ByteString.copyFrom(genUUID());
    Key minKey = Key.toRawKey(sortedList.get(0).first);
    Key maxKey = Key.toRawKey(sortedList.get(sortedList.size() - 1).first);
    ImporterClient importerClient = new ImporterClient(tiSession, uuid, minKey, maxKey, region, 0L);
    importerClient.write(sortedList.iterator());
  }
}
