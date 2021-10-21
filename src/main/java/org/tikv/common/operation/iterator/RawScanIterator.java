/*
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
 */

package org.tikv.common.operation.iterator;

import com.google.protobuf.ByteString;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb;

public class RawScanIterator extends ScanIterator {

  public RawScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      int limit,
      boolean keyOnly) {
    super(conf, builder, startKey, endKey, limit, keyOnly);
  }

  TiRegion loadCurrentRegionToCache() throws GrpcException {
    BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
    while (true) {
      try (RegionStoreClient client = builder.build(startKey)) {
        client.setTimeout(conf.getScanTimeout());
        TiRegion region = client.getRegion();
        if (limit <= 0) {
          currentCache = null;
        } else {
          try {
            currentCache =
                client.rawScan(backOffer, startKey, endKey.toByteString(), limit, keyOnly);
          } catch (final TiKVException e) {
            backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
            continue;
          }
        }
        return region;
      }
    }
  }

  private boolean notEndOfScan() {
    return limit > 0
        && !(processingLastBatch
            && (index >= currentCache.size()
                || Key.toRawKey(currentCache.get(index).getKey()).compareTo(endKey) >= 0));
  }

  boolean isCacheDrained() {
    return currentCache == null || limit <= 0 || index >= currentCache.size() || index == -1;
  }

  @Override
  public boolean hasNext() {
    if (isCacheDrained() && cacheLoadFails()) {
      endOfScan = true;
      return false;
    }
    // continue when cache is empty but not null
    while (currentCache != null && currentCache.isEmpty()) {
      if (cacheLoadFails()) {
        return false;
      }
    }
    return notEndOfScan();
  }

  private Kvrpcpb.KvPair getCurrent() {
    --limit;
    return currentCache.get(index++);
  }

  @Override
  public Kvrpcpb.KvPair next() {
    return getCurrent();
  }
}
