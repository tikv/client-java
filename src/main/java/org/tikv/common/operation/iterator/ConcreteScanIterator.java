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

package org.tikv.common.operation.iterator;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.KeyException;
import org.tikv.common.key.Key;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;

public class ConcreteScanIterator extends ScanIterator {
  private final long version;
  private final Logger logger = LoggerFactory.getLogger(ConcreteScanIterator.class);

  public ConcreteScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      long version,
      int limit) {
    // Passing endKey as ByteString.EMPTY means that endKey is +INF by default,
    this(conf, builder, startKey, ByteString.EMPTY, version, limit);
  }

  public ConcreteScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    // Passing endKey as ByteString.EMPTY means that endKey is +INF by default,
    this(conf, builder, startKey, endKey, version, Integer.MAX_VALUE);
  }

  private ConcreteScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version,
      int limit) {
    super(conf, builder, startKey, endKey, limit);
    this.version = version;
  }

  @Override
  TiRegion loadCurrentRegionToCache() throws GrpcException {
    TiRegion region;
    try (RegionStoreClient client = builder.build(startKey)) {
      region = client.getRegion();
      BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
      currentCache = client.scan(backOffer, startKey, version);
      return region;
    }
  }

  private ByteString resolveCurrentLock(Kvrpcpb.KvPair current) {
    logger.warn(String.format("resolve current key error %s", current.getError().toString()));
    Pair<TiRegion, Metapb.Store> pair =
        builder.getRegionManager().getRegionStorePairByKey(current.getKey());
    TiRegion region = pair.first;
    Metapb.Store store = pair.second;
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
    try (RegionStoreClient client = builder.build(region, store)) {
      return client.get(backOffer, current.getKey(), version);
    } catch (Exception e) {
      throw new KeyException(current.getError());
    }
  }

  @Override
  public boolean hasNext() {
    Kvrpcpb.KvPair current;
    // continue when cache is empty but not null
    do {
      current = getCurrent();
      if (isCacheDrained() && cacheLoadFails()) {
        endOfScan = true;
        return false;
      }
    } while (currentCache != null && current == null);
    // for last batch to be processed, we have to check if
    return !processingLastBatch
        || current == null
        || (hasEndKey && Key.toRawKey(current.getKey()).compareTo(endKey) < 0);
  }

  @Override
  public Kvrpcpb.KvPair next() {
    --limit;
    Kvrpcpb.KvPair current = currentCache.get(index++);

    requireNonNull(current, "current kv pair cannot be null");
    if (current.hasError()) {
      ByteString val = resolveCurrentLock(current);
      current = Kvrpcpb.KvPair.newBuilder().setKey(current.getKey()).setValue(val).build();
    }

    return current;
  }

  /**
   * Cache is drained when - no data extracted - scan limit was not defined - have read the last
   * index of cache - index not initialized
   *
   * @return whether cache is drained
   */
  private boolean isCacheDrained() {
    return currentCache == null || limit <= 0 || index >= currentCache.size() || index == -1;
  }

  private Kvrpcpb.KvPair getCurrent() {
    if (isCacheDrained()) {
      return null;
    }
    return currentCache.get(index);
  }
}
