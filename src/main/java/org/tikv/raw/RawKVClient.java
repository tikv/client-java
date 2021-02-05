/*
 * Copyright 2018 PingCAP, Inc.
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

package org.tikv.raw;

import static org.tikv.common.util.ClientUtils.*;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.operation.iterator.RawScanIterator;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.*;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class RawKVClient implements AutoCloseable {
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final ExecutorService batchGetThreadPool;
  private final ExecutorService batchPutThreadPool;
  private final ExecutorService batchScanThreadPool;
  private final ExecutorService deleteRangeThreadPool;
  private static final Logger logger = LoggerFactory.getLogger(RawKVClient.class);

  private static final int MAX_RETRY_LIMIT = 3;
  // https://www.github.com/pingcap/tidb/blob/master/store/tikv/rawkv.go
  private static final int MAX_RAW_SCAN_LIMIT = 10240;
  private static final int RAW_BATCH_PUT_SIZE = 16 * 1024;
  private static final int RAW_BATCH_GET_SIZE = 16 * 1024;
  private static final int RAW_BATCH_SCAN_SIZE = 16;
  private static final int RAW_BATCH_PAIR_COUNT = 512;

  private static final TiKVException ERR_RETRY_LIMIT_EXCEEDED =
      new GrpcException("retry is exhausted. retry exceeds " + MAX_RETRY_LIMIT + "attempts");
  private static final TiKVException ERR_MAX_SCAN_LIMIT_EXCEEDED =
      new TiKVException("limit should be less than MAX_RAW_SCAN_LIMIT");

  public RawKVClient(TiSession session, RegionStoreClientBuilder clientBuilder) {
    Objects.requireNonNull(session, "session is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.conf = session.getConf();
    this.clientBuilder = clientBuilder;
    this.batchGetThreadPool = session.getThreadPoolForBatchGet();
    this.batchPutThreadPool = session.getThreadPoolForBatchPut();
    this.batchScanThreadPool = session.getThreadPoolForBatchScan();
    this.deleteRangeThreadPool = session.getThreadPoolForDeleteRange();
  }

  @Override
  public void close() {}

  /**
   * Put a raw key-value pair to TiKV
   *
   * @param key raw key
   * @param value raw value
   */
  public void put(ByteString key, ByteString value) {
    BackOffer backOffer = defaultBackOff();
    for (int i = 0; i < MAX_RETRY_LIMIT; i++) {
      RegionStoreClient client = clientBuilder.build(key);
      try {
        client.rawPut(backOffer, key, value);
        return;
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
    throw ERR_RETRY_LIMIT_EXCEEDED;
  }

  /**
   * Put a set of raw key-value pair to TiKV
   *
   * @param kvPairs kvPairs
   */
  public void batchPut(Map<ByteString, ByteString> kvPairs) {
    doSendBatchPut(ConcreteBackOffer.newRawKVBackOff(), kvPairs);
  }

  private void batchPut(BackOffer backOffer, List<ByteString> keys, List<ByteString> values) {
    Map<ByteString, ByteString> keysToValues = mapKeysToValues(keys, values);
    doSendBatchPut(backOffer, keysToValues);
  }

  /**
   * Get a raw key-value pair from TiKV if key exists
   *
   * @param key raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString get(ByteString key) {
    BackOffer backOffer = defaultBackOff();
    for (int i = 0; i < MAX_RETRY_LIMIT; i++) {
      RegionStoreClient client = clientBuilder.build(key);
      try {
        return client.rawGet(defaultBackOff(), key);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
    throw ERR_RETRY_LIMIT_EXCEEDED;
  }

  /**
   * Get a list of raw key-value pair from TiKV if key exists
   *
   * @param keys list of raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public List<KvPair> batchGet(List<ByteString> keys) {
    BackOffer backOffer = defaultBackOff();
    return doSendBatchGet(backOffer, keys);
  }

  public List<List<KvPair>> batchScan(List<ScanOption> ranges) {
    if (ranges.isEmpty()) {
      return new ArrayList<>();
    }
    ExecutorCompletionService<Pair<Integer, List<KvPair>>> completionService =
        new ExecutorCompletionService<>(batchScanThreadPool);
    int num = 0;
    for (ScanOption scanOption : ranges) {
      int i = num;
      completionService.submit(() -> Pair.create(i, scan(scanOption)));
      ++num;
    }
    List<List<KvPair>> scanResults = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      scanResults.add(new ArrayList<>());
    }
    for (int i = 0; i < num; i++) {
      try {
        Pair<Integer, List<KvPair>> scanResult =
            completionService.take().get(BackOffer.RAWKV_MAX_BACKOFF, TimeUnit.SECONDS);
        scanResults.set(scanResult.first, scanResult.second);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TiKVException("Current thread interrupted.", e);
      } catch (TimeoutException e) {
        throw new TiKVException("TimeOut Exceeded for current operation. ", e);
      } catch (ExecutionException e) {
        throw new TiKVException("Execution exception met.", e);
      }
    }
    return scanResults;
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit) {
    return scan(startKey, endKey, limit, false);
  }

  public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    Iterator<KvPair> iterator =
        rawScanIterator(conf, clientBuilder, startKey, endKey, limit, keyOnly);
    List<KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, int limit) {
    return scan(startKey, limit, false);
  }

  public List<KvPair> scan(ByteString startKey, int limit, boolean keyOnly) {
    return scan(startKey, ByteString.EMPTY, limit, keyOnly);
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, ByteString endKey) {
    return scan(startKey, endKey, false);
  }

  public List<KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly) {
    List<KvPair> result = new ArrayList<>();
    while (true) {
      Iterator<KvPair> iterator =
          rawScanIterator(conf, clientBuilder, startKey, endKey, conf.getScanBatchSize(), keyOnly);
      if (!iterator.hasNext()) {
        break;
      }
      iterator.forEachRemaining(result::add);
      startKey = Key.toRawKey(result.get(result.size() - 1).getKey()).next().toByteString();
    }
    return result;
  }

  private List<KvPair> scan(ScanOption scanOption) {
    ByteString startKey = scanOption.getStartKey();
    ByteString endKey = scanOption.getEndKey();
    int limit = scanOption.getLimit();
    boolean keyOnly = scanOption.isKeyOnly();
    return scan(startKey, endKey, limit, keyOnly);
  }

  /**
   * Delete a raw key-value pair from TiKV if key exists
   *
   * @param key raw key to be deleted
   */
  public void delete(ByteString key) {
    BackOffer backOffer = defaultBackOff();
    while (true) {
      RegionStoreClient client = clientBuilder.build(key);
      try {
        client.rawDelete(defaultBackOff(), key);
        return;
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
  }

  /**
   * Delete all raw key-value pairs in range [startKey, endKey) from TiKV
   *
   * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
   * range along with deleteRange API, the result will be undefined.
   *
   * @param startKey raw start key to be deleted
   * @param endKey raw start key to be deleted
   */
  public synchronized void deleteRange(ByteString startKey, ByteString endKey) {
    BackOffer backOffer = defaultBackOff();
    doSendDeleteRange(backOffer, startKey, endKey);
  }

  /**
   * Delete all raw key-value pairs with the prefix `key` from TiKV
   *
   * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
   * range along with deleteRange API, the result will be undefined.
   *
   * @param key prefix of keys to be deleted
   */
  public synchronized void deletePrefix(ByteString key) {
    ByteString endKey = Key.toRawKey(key).nextPrefix().toByteString();
    deleteRange(key, endKey);
  }

  /**
   * Append batch to list and split them according to batch limit
   *
   * @param batches a grouped batch
   * @param region region
   * @param keys keys
   * @param values values
   * @param limit batch max limit
   */
  private void appendBatches(
      List<Batch> batches,
      TiRegion region,
      List<ByteString> keys,
      List<ByteString> values,
      int limit) {
    List<ByteString> tmpKeys = new ArrayList<>();
    List<ByteString> tmpValues = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      if (i >= limit) {
        batches.add(new Batch(region, tmpKeys, tmpValues));
        tmpKeys.clear();
        tmpValues.clear();
      }
      tmpKeys.add(keys.get(i));
      tmpValues.add(values.get(i));
    }
    if (!tmpKeys.isEmpty()) {
      batches.add(new Batch(region, tmpKeys, tmpValues));
    }
  }

  private void appendBatches(
      List<Batch> batches, TiRegion region, List<ByteString> keys, int limit) {
    List<ByteString> tmpKeys = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      if (i >= limit) {
        batches.add(new Batch(region, tmpKeys));
        tmpKeys.clear();
      }
      tmpKeys.add(keys.get(i));
    }
    if (!tmpKeys.isEmpty()) {
      batches.add(new Batch(region, tmpKeys));
    }
  }

  private void doSendBatchPut(BackOffer backOffer, Map<ByteString, ByteString> kvPairs) {
    ExecutorCompletionService<Object> completionService =
        new ExecutorCompletionService<>(batchPutThreadPool);

    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(kvPairs.keySet());
    List<Batch> batches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          batches,
          entry.getKey(),
          entry.getValue(),
          entry.getValue().stream().map(kvPairs::get).collect(Collectors.toList()),
          RAW_BATCH_PUT_SIZE);
    }

    for (Batch batch : batches) {
      BackOffer singleBatchBackOffer = ConcreteBackOffer.create(backOffer);
      completionService.submit(() -> doSendBatchPutInBatchesWithRetry(singleBatchBackOffer, batch));
    }
    getTasks(completionService, batches, BackOffer.RAWKV_MAX_BACKOFF);
  }

  private Object doSendBatchPutInBatchesWithRetry(BackOffer backOffer, Batch batch) {
    TiRegion oldRegion = batch.region;
    TiRegion currentRegion =
        clientBuilder.getRegionManager().getRegionByKey(oldRegion.getStartKey());

    if (oldRegion.equals(currentRegion)) {
      try (RegionStoreClient client = clientBuilder.build(batch.region); ) {
        client.rawBatchPut(backOffer, batch);
        return null;
      } catch (final TiKVException e) {
        // TODO: any elegant way to re-split the ranges if fails?
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        logger.warn("ReSplitting ranges for BatchPutRequest");
        // retry
        return doSendBatchPutWithRefetchRegion(backOffer, batch);
      }
    } else {
      return doSendBatchPutWithRefetchRegion(backOffer, batch);
    }
  }

  private Object doSendBatchPutWithRefetchRegion(BackOffer backOffer, Batch batch) {
    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(batch.keys);
    List<Batch> retryBatches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          retryBatches,
          entry.getKey(),
          entry.getValue(),
          entry.getValue().stream().map(batch.map::get).collect(Collectors.toList()),
          RAW_BATCH_PUT_SIZE);
    }

    for (Batch retryBatch : retryBatches) {
      // recursive calls
      doSendBatchPutInBatchesWithRetry(backOffer, retryBatch);
    }

    return null;
  }

  private List<KvPair> doSendBatchGet(BackOffer backOffer, List<ByteString> keys) {
    ExecutorCompletionService<List<KvPair>> completionService =
        new ExecutorCompletionService<>(batchGetThreadPool);

    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(keys);
    List<Batch> batches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(batches, entry.getKey(), entry.getValue(), RAW_BATCH_GET_SIZE);
    }

    for (Batch batch : batches) {
      BackOffer singleBatchBackOffer = ConcreteBackOffer.create(backOffer);
      completionService.submit(() -> doSendBatchGetInBatchesWithRetry(singleBatchBackOffer, batch));
    }

    return getKvPairs(completionService, batches, BackOffer.RAWKV_MAX_BACKOFF);
  }

  private List<KvPair> doSendBatchGetInBatchesWithRetry(BackOffer backOffer, Batch batch) {
    TiRegion oldRegion = batch.region;
    TiRegion currentRegion =
        clientBuilder.getRegionManager().getRegionByKey(oldRegion.getStartKey());

    if (oldRegion.equals(currentRegion)) {
      RegionStoreClient client = clientBuilder.build(batch.region);
      try {
        return client.rawBatchGet(backOffer, batch.keys);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        clientBuilder.getRegionManager().invalidateRegion(batch.region.getId());
        logger.warn("ReSplitting ranges for BatchGetRequest", e);

        // retry
        return doSendBatchGetWithRefetchRegion(backOffer, batch);
      }
    } else {
      return doSendBatchGetWithRefetchRegion(backOffer, batch);
    }
  }

  private List<KvPair> doSendBatchGetWithRefetchRegion(BackOffer backOffer, Batch batch) {
    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(batch.keys);
    List<Batch> retryBatches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(retryBatches, entry.getKey(), entry.getValue(), RAW_BATCH_GET_SIZE);
    }

    ArrayList<KvPair> results = new ArrayList<>();
    for (Batch retryBatch : retryBatches) {
      // recursive calls
      List<KvPair> batchResult = doSendBatchGetInBatchesWithRetry(backOffer, retryBatch);
      results.addAll(batchResult);
    }
    return results;
  }

  private ByteString calcKeyByCondition(boolean condition, ByteString key1, ByteString key2) {
    if (condition) {
      return key1;
    }
    return key2;
  }

  private void doSendDeleteRange(BackOffer backOffer, ByteString startKey, ByteString endKey) {
    ExecutorCompletionService<Object> completionService =
        new ExecutorCompletionService<>(deleteRangeThreadPool);

    List<TiRegion> regions = fetchRegionsFromRange(startKey, endKey);
    for (int i = 0; i < regions.size(); i++) {
      TiRegion region = regions.get(i);
      BackOffer singleBatchBackOffer = ConcreteBackOffer.create(backOffer);
      ByteString start = calcKeyByCondition(i == 0, startKey, region.getStartKey());
      ByteString end = calcKeyByCondition(i == regions.size() - 1, endKey, region.getEndKey());
      completionService.submit(
          () -> doSendDeleteRangeWithRetry(singleBatchBackOffer, region, start, end));
    }
    for (int i = 0; i < regions.size(); i++) {
      try {
        completionService.take().get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TiKVException("Current thread interrupted.", e);
      } catch (ExecutionException e) {
        throw new TiKVException("Execution exception met.", e);
      }
    }
  }

  private Object doSendDeleteRangeWithRetry(
      BackOffer backOffer, TiRegion region, ByteString startKey, ByteString endKey) {
    TiRegion currentRegion = clientBuilder.getRegionManager().getRegionByKey(region.getStartKey());

    if (region.equals(currentRegion)) {
      RegionStoreClient client = clientBuilder.build(region);
      try {
        client.rawDeleteRange(backOffer, startKey, endKey);
        return null;
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        clientBuilder.getRegionManager().invalidateRegion(region.getId());
        logger.warn("ReSplitting ranges for BatchDeleteRangeRequest", e);

        // retry
        return doSendDeleteRangeWithRefetchRegion(backOffer, startKey, endKey);
      }
    } else {
      return doSendDeleteRangeWithRefetchRegion(backOffer, startKey, endKey);
    }
  }

  private Object doSendDeleteRangeWithRefetchRegion(
      BackOffer backOffer, ByteString startKey, ByteString endKey) {
    List<TiRegion> regions = fetchRegionsFromRange(startKey, endKey);
    for (int i = 0; i < regions.size(); i++) {
      TiRegion region = regions.get(i);
      ByteString start = calcKeyByCondition(i == 0, startKey, region.getStartKey());
      ByteString end = calcKeyByCondition(i == regions.size() - 1, endKey, region.getEndKey());
      doSendDeleteRangeWithRetry(backOffer, region, start, end);
    }
    return null;
  }

  /**
   * Group by list of keys according to its region
   *
   * @param keys keys
   * @return a mapping of keys and their region
   */
  private Map<TiRegion, List<ByteString>> groupKeysByRegion(Set<ByteString> keys) {
    Map<TiRegion, List<ByteString>> groups = new HashMap<>();
    TiRegion lastRegion = null;
    for (ByteString key : keys) {
      if (lastRegion == null || !lastRegion.contains(key)) {
        lastRegion = clientBuilder.getRegionManager().getRegionByKey(key);
      }
      groups.computeIfAbsent(lastRegion, k -> new ArrayList<>()).add(key);
    }
    return groups;
  }

  private Map<TiRegion, List<ByteString>> groupKeysByRegion(List<ByteString> keys) {
    return keys.stream()
        .collect(Collectors.groupingBy(clientBuilder.getRegionManager()::getRegionByKey));
  }

  private static Map<ByteString, ByteString> mapKeysToValues(
      List<ByteString> keys, List<ByteString> values) {
    Map<ByteString, ByteString> map = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      map.put(keys.get(i), values.get(i));
    }
    return map;
  }

  private List<TiRegion> fetchRegionsFromRange(ByteString startKey, ByteString endKey) {
    List<TiRegion> regions = new ArrayList<>();
    while (FastByteComparisons.compareTo(startKey.toByteArray(), endKey.toByteArray()) < 0) {
      TiRegion currentRegion = clientBuilder.getRegionManager().getRegionByKey(startKey);
      regions.add(currentRegion);
      startKey = currentRegion.getEndKey();
    }
    return regions;
  }

  private Iterator<KvPair> rawScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      int limit,
      boolean keyOnly) {
    if (limit > MAX_RAW_SCAN_LIMIT) {
      throw ERR_MAX_SCAN_LIMIT_EXCEEDED;
    }
    return new RawScanIterator(conf, builder, startKey, endKey, limit, keyOnly);
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }
}
