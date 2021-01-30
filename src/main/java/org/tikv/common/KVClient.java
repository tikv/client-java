/*
 *
 * Copyright 2019 PingCAP, Inc.
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

package org.tikv.common;

import static org.tikv.common.util.ClientUtils.getKvPairs;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.operation.iterator.ConcreteScanIterator;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.Batch;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class KVClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(KVClient.class);
  private static final int BATCH_GET_SIZE = 16 * 1024;
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final ExecutorService batchGetThreadPool;

  public KVClient(TiSession session, RegionStoreClientBuilder clientBuilder) {
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.conf = session.getConf();
    this.clientBuilder = clientBuilder;
    this.batchGetThreadPool = session.getThreadPoolForBatchGet();
  }

  @Override
  public void close() {}

  /**
   * Get a key-value pair from TiKV if key exists
   *
   * @param key key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString get(ByteString key, long version) throws GrpcException {
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
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
  public List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version)
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
  public List<KvPair> scan(ByteString startKey, ByteString endKey, long version)
      throws GrpcException {
    Iterator<KvPair> iterator = scanIterator(conf, clientBuilder, startKey, endKey, version);
    List<KvPair> result = new ArrayList<>();
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
  public List<KvPair> scan(ByteString startKey, long version, int limit) throws GrpcException {
    Iterator<KvPair> iterator = scanIterator(conf, clientBuilder, startKey, version, limit);
    List<KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  public List<KvPair> scan(ByteString startKey, long version) throws GrpcException {
    return scan(startKey, version, Integer.MAX_VALUE);
  }

  private List<KvPair> doSendBatchGet(BackOffer backOffer, List<ByteString> keys, long version) {
    ExecutorCompletionService<List<KvPair>> completionService =
        new ExecutorCompletionService<>(batchGetThreadPool);

    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(keys);
    List<Batch> batches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(batches, entry.getKey(), entry.getValue(), BATCH_GET_SIZE);
    }

    for (Batch batch : batches) {
      BackOffer singleBatchBackOffer = ConcreteBackOffer.create(backOffer);
      completionService.submit(
          () -> doSendBatchGetInBatchesWithRetry(singleBatchBackOffer, batch, version));
    }

    return getKvPairs(completionService, batches);
  }

  private List<KvPair> doSendBatchGetInBatchesWithRetry(
      BackOffer backOffer, Batch batch, long version) {
    TiRegion oldRegion = batch.region;
    TiRegion currentRegion =
        clientBuilder.getRegionManager().getRegionByKey(oldRegion.getStartKey());

    if (oldRegion.equals(currentRegion)) {
      RegionStoreClient client = clientBuilder.build(batch.region);
      try {
        return client.batchGet(backOffer, batch.keys, version);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        clientBuilder.getRegionManager().invalidateRegion(batch.region.getId());
        logger.warn("ReSplitting ranges for BatchGetRequest", e);

        // retry
        return doSendBatchGetWithRefetchRegion(backOffer, batch, version);
      }
    } else {
      return doSendBatchGetWithRefetchRegion(backOffer, batch, version);
    }
  }

  private List<KvPair> doSendBatchGetWithRefetchRegion(
      BackOffer backOffer, Batch batch, long version) {
    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(batch.keys);
    List<Batch> retryBatches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(retryBatches, entry.getKey(), entry.getValue(), BATCH_GET_SIZE);
    }

    ArrayList<KvPair> results = new ArrayList<>();
    for (Batch retryBatch : retryBatches) {
      // recursive calls
      List<KvPair> batchResult = doSendBatchGetInBatchesWithRetry(backOffer, retryBatch, version);
      results.addAll(batchResult);
    }
    return results;
  }

  /**
   * Append batch to list and split them according to batch limit
   *
   * @param batches a grouped batch
   * @param region region
   * @param keys keys
   * @param batchGetMaxSizeInByte batch max limit
   */
  private void appendBatches(
      List<Batch> batches, TiRegion region, List<ByteString> keys, int batchGetMaxSizeInByte) {
    int start;
    int end;
    if (keys == null) {
      return;
    }
    int len = keys.size();
    for (start = 0; start < len; start = end) {
      int size = 0;
      for (end = start; end < len && size < batchGetMaxSizeInByte; end++) {
        size += keys.get(end).size();
      }
      Batch batch = new Batch(region, keys.subList(start, end));
      batches.add(batch);
    }
  }

  /**
   * Group by list of keys according to its region
   *
   * @param keys keys
   * @return a mapping of keys and their region
   */
  private Map<TiRegion, List<ByteString>> groupKeysByRegion(List<ByteString> keys) {
    return keys.stream()
        .collect(Collectors.groupingBy(clientBuilder.getRegionManager()::getRegionByKey));
  }

  private Iterator<KvPair> scanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    return new ConcreteScanIterator(conf, builder, startKey, endKey, version);
  }

  private Iterator<KvPair> scanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      long version,
      int limit) {
    return new ConcreteScanIterator(conf, builder, startKey, version, limit);
  }
}
