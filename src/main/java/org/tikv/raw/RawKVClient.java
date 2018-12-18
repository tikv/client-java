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

import java.util.*;

public class RawKVClient implements AutoCloseable {
  private static final String DEFAULT_PD_ADDRESS = "127.0.0.1:2379";
  private final TiSession session;
  private final RegionManager regionManager;

  private RawKVClient(String addresses) {
    session = TiSession.create(TiConfiguration.createRawDefault(addresses));
    regionManager = session.getRegionManager();
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

  /**
   * Put a list of raw key-value pair to TiKV
   *
   * @param kvPairs kvPairs
   */
  public void batchPut(List<Kvrpcpb.KvPair> kvPairs) {
    BackOffer backOffer = defaultBackOff();
    List<Kvrpcpb.KvPair> remainingPairs = new ArrayList<>();
    while (true) {
      kvPairs.addAll(remainingPairs);
      remainingPairs.clear();
      Map<Pair<TiRegion, Metapb.Store>, List<Kvrpcpb.KvPair>> regionMap = new HashMap<>();
      for (Kvrpcpb.KvPair kvPair : kvPairs) {
        Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByKey(kvPair.getKey());
        regionMap.computeIfAbsent(pair, t -> new ArrayList<>()).add(kvPair);
      }

      for (Map.Entry<Pair<TiRegion, Metapb.Store>, List<Kvrpcpb.KvPair>> entry :
          regionMap.entrySet()) {
        RegionStoreClient client =
            RegionStoreClient.create(entry.getKey().first, entry.getKey().second, session);
        try {
          client.rawBatchPut(defaultBackOff(), entry.getValue());
        } catch (final TiKVException e) {
          remainingPairs.addAll(entry.getValue());
        }
      }
      if (remainingPairs.isEmpty()) {
        return;
      }
      // re-splitting ranges
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss,
          new TiKVException("BatchPut encounter exception, need re-split the ranges"));
      kvPairs.clear();
    }
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
