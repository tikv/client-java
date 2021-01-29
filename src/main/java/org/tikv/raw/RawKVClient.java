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
    batchPut(ConcreteBackOffer.newRawKVBackOff(), kvPairs);
  }

  private void batchPut(BackOffer backOffer, List<ByteString> keys, List<ByteString> values) {
    Map<ByteString, ByteString> keysToValues = mapKeysToValues(keys, values);
    batchPut(backOffer, keysToValues);
  }

  private void batchPut(BackOffer backOffer, Map<ByteString, ByteString> kvPairs) {
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
    sendBatchPut(backOffer, batches);
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
    return batchGet(backOffer, keys);
  }

  private List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys) {
    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(keys);
    List<Batch> batches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(batches, entry.getKey(), entry.getValue(), RAW_BATCH_GET_SIZE);
    }
    return sendBatchGet(backOffer, batches);
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
        batches.add(new Batch(region, tmpKeys, new ArrayList<>()));
        tmpKeys.clear();
      }
      tmpKeys.add(keys.get(i));
    }
    if (!tmpKeys.isEmpty()) {
      batches.add(new Batch(region, tmpKeys, new ArrayList<>()));
    }
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

  /**
   * Send batchPut request concurrently
   *
   * @param backOffer current backOffer
   * @param batches list of batch to send
   */
  private List<KvPair> sendBatchGet(BackOffer backOffer, List<Batch> batches) {
    if (batches.isEmpty()) {
      return new ArrayList<>();
    }
    ExecutorCompletionService<List<KvPair>> completionService =
        new ExecutorCompletionService<>(batchGetThreadPool);
    for (Batch batch : batches) {
      completionService.submit(
          () -> {
            BackOffer singleBatchBackOffer = ConcreteBackOffer.create(backOffer);
            try (RegionStoreClient client = clientBuilder.build(batch.region); ) {
              return client.rawBatchGet(singleBatchBackOffer, batch.keys);
            } catch (final TiKVException e) {
              // TODO: any elegant way to re-split the ranges if fails?
              singleBatchBackOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
              logger.warn("ReSplitting ranges for BatchGetRequest");
              // recursive calls
              batchGet(singleBatchBackOffer, batch.keys);
            }
            return null;
          });
    }
    try {
      List<KvPair> results = new ArrayList<>();
      for (int i = 0; i < batches.size(); i++) {
        results.addAll(completionService.take().get(BackOffer.RAWKV_MAX_BACKOFF, TimeUnit.SECONDS));
      }
      return results;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (TimeoutException e) {
      throw new TiKVException("TimeOut Exceeded for current operation. ", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
  }

  /**
   * Send batchPut request concurrently
   *
   * @param backOffer current backOffer
   * @param batches list of batch to send
   */
  private void sendBatchPut(BackOffer backOffer, List<Batch> batches) {
    ExecutorCompletionService<Object> completionService =
        new ExecutorCompletionService<>(batchPutThreadPool);
    for (Batch batch : batches) {
      completionService.submit(
          () -> {
            BackOffer singleBatchBackOffer = ConcreteBackOffer.create(backOffer);
            List<KvPair> kvPairs = new ArrayList<>();
            for (int i = 0; i < batch.keys.size(); i++) {
              kvPairs.add(
                  KvPair.newBuilder()
                      .setKey(batch.keys.get(i))
                      .setValue(batch.values.get(i))
                      .build());
            }
            try (RegionStoreClient client = clientBuilder.build(batch.region); ) {
              client.rawBatchPut(singleBatchBackOffer, kvPairs);
            } catch (final TiKVException e) {
              // TODO: any elegant way to re-split the ranges if fails?
              singleBatchBackOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
              logger.warn("ReSplitting ranges for BatchPutRequest");
              // recursive calls
              batchPut(singleBatchBackOffer, batch.keys, batch.values);
            }
            return null;
          });
    }
    try {
      for (int i = 0; i < batches.size(); i++) {
        completionService.take().get(BackOffer.RAWKV_MAX_BACKOFF, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (TimeoutException e) {
      throw new TiKVException("TimeOut Exceeded for current operation. ", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
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
