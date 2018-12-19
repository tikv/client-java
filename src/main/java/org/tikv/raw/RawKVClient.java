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
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.operation.iterator.RawScanIterator;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;

public class RawKVClient implements AutoCloseable {
  private static final String DEFAULT_PD_ADDRESS = "127.0.0.1:2379";
  private final TiSession session;
  private final RegionManager regionManager;
  private final ExecutorCompletionService<Object> completionService;
  private static final Logger logger = Logger.getLogger(RawKVClient.class);

  private static final int RAW_BATCH_PUT_SIZE = 16 * 1024;

  private RawKVClient(String addresses) {
    session = TiSession.create(TiConfiguration.createRawDefault(addresses));
    regionManager = session.getRegionManager();
    ExecutorService executors =
        Executors.newFixedThreadPool(session.getConf().getRawClientConcurrency());
    completionService = new ExecutorCompletionService<>(executors);
  }

  private RawKVClient() {
    this(DEFAULT_PD_ADDRESS);
  }

  public static RawKVClient create() {
    return new RawKVClient();
  }

  public static RawKVClient create(String address) {
    return new RawKVClient(address);
  }

  @Override
  public void close() {
    session.close();
  }

  /**
   * Put a raw key-value pair to TiKV
   *
   * @param key raw key
   * @param value raw value
   */
  public void put(ByteString key, ByteString value) {
    BackOffer backOffer = defaultBackOff();
    while (true) {
      Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByKey(key);
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      try {
        client.rawPut(backOffer, key, value);
        return;
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
  }

  public void batchPut(List<ByteString> keys, List<ByteString> values) {
    batchPut(ConcreteBackOffer.newRawKVBackOff(), keys, values);
  }

  /**
   * Put a list of raw key-value pair to TiKV
   *
   * @param keys keys
   * @param values values
   */
  public void batchPut(BackOffer backOffer, List<ByteString> keys, List<ByteString> values) {
    assert keys.size() == values.size();
    Map<ByteString, ByteString> keysToValues = mapKeysToValues(keys, values);
    Map<Long, List<ByteString>> groupKeys = groupKeysByRegion(keys);
    keys.clear();
    List<Batch> batches = new ArrayList<>();

    for (Map.Entry<Long, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          batches,
          entry.getKey(),
          entry.getValue(),
          entry.getValue().stream().map(keysToValues::get).collect(Collectors.toList()),
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
    while (true) {
      Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByKey(key);
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      try {
        return client.rawGet(defaultBackOff(), key);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return list of key-value pairs in range
   */
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey) {
    Iterator<Kvrpcpb.KvPair> iterator = rawScanIterator(startKey, endKey);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs
   * @return list of key-value pairs in range
   */
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit) {
    Iterator<Kvrpcpb.KvPair> iterator = rawScanIterator(startKey, limit);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  /**
   * Delete a raw key-value pair from TiKV if key exists
   *
   * @param key raw key to be deleted
   */
  public void delete(ByteString key) {
    BackOffer backOffer = defaultBackOff();
    while (true) {
      TiRegion region = regionManager.getRegionByKey(key);
      Kvrpcpb.Context context =
          Kvrpcpb.Context.newBuilder()
              .setRegionId(region.getId())
              .setRegionEpoch(region.getRegionEpoch())
              .setPeer(region.getLeader())
              .build();
      Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByKey(key);
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      try {
        client.rawDelete(defaultBackOff(), key, context);
        return;
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
  }

  private class Batch {
    private long regionId;
    private List<ByteString> keys;
    private List<ByteString> values;

    public Batch(long regionId, List<ByteString> keys, List<ByteString> values) {
      this.regionId = regionId;
      this.keys = keys;
      this.values = values;
    }
  }

  /**
   * Append batch to list and split them according to batch limit
   *
   * @param batches a grouped batch
   * @param regionId region ID
   * @param keys keys
   * @param values values
   * @param limit batch max limit
   */
  private void appendBatches(
      List<Batch> batches,
      long regionId,
      List<ByteString> keys,
      List<ByteString> values,
      int limit) {
    List<ByteString> tmpKeys = new ArrayList<>();
    List<ByteString> tmpValues = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      if (i >= limit) {
        batches.add(new Batch(regionId, tmpKeys, tmpValues));
        tmpKeys.clear();
        tmpValues.clear();
      }
      tmpKeys.add(keys.get(i));
      tmpValues.add(values.get(i));
    }
    if (!tmpKeys.isEmpty()) {
      batches.add(new Batch(regionId, tmpKeys, tmpValues));
    }
  }

  /**
   * Group by list of keys according to its region
   *
   * @param keys keys
   * @return a mapping of keys and their regionId
   */
  private Map<Long, List<ByteString>> groupKeysByRegion(List<ByteString> keys) {
    Map<Long, List<ByteString>> groups = new HashMap<>();
    TiRegion lastRegion = null;
    for (ByteString key : keys) {
      if (lastRegion == null || !lastRegion.contains(key)) {
        lastRegion = regionManager.getRegionByKey(key);
      }
      groups.computeIfAbsent(lastRegion.getId(), k -> new ArrayList<>()).add(key);
    }
    return groups;
  }

  static Map<ByteString, ByteString> mapKeysToValues(
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
  private void sendBatchPut(BackOffer backOffer, List<Batch> batches) {
    for (Batch batch : batches) {
      completionService.submit(
          () -> {
            Pair<TiRegion, Metapb.Store> pair =
                regionManager.getRegionStorePairByRegionId(batch.regionId);
            RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
            List<Kvrpcpb.KvPair> kvPairs = new ArrayList<>();
            for (int i = 0; i < batch.keys.size(); i++) {
              kvPairs.add(
                  Kvrpcpb.KvPair.newBuilder()
                      .setKey(batch.keys.get(i))
                      .setValue(batch.values.get(i))
                      .build());
            }
            try {
              client.rawBatchPut(backOffer, kvPairs);
            } catch (final TiKVException e) {
              // TODO: any elegant way to re-split the ranges if fails?
              backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
              logger.warn("ReSplitting ranges for BatchPutRequest");
              // recursive calls
              batchPut(backOffer, batch.keys, batch.values);
            }
            return null;
          });
    }
    try {
      for (int i = 0; i < batches.size(); i++) {
        completionService.take().get(BackOffer.rawkvMaxBackoff, TimeUnit.SECONDS);
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

  private Iterator<Kvrpcpb.KvPair> rawScanIterator(ByteString startKey, ByteString endKey) {
    return new RawScanIterator(startKey, endKey, Integer.MAX_VALUE, session);
  }

  private Iterator<Kvrpcpb.KvPair> rawScanIterator(ByteString startKey, int limit) {
    return new RawScanIterator(startKey, ByteString.EMPTY, limit, session);
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }
}
