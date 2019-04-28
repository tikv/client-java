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
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;

public class RawScanIterator extends ScanIterator {
  private ByteString cf;

  public RawScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString cf,
      ByteString startKey,
      ByteString endKey,
      int limit) {
    super(conf, builder, startKey, endKey, limit);
    this.cf = cf;
  }

  TiRegion loadCurrentRegionToCache() throws Exception {
    TiRegion region;
    try (RegionStoreClient client = builder.build(startKey)) {
      region = client.getRegion();
      BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
      if (limit <= 0) {
        currentCache = null;
      } else {
        while (true) {
          try {
            currentCache = client.rawScan(backOffer, startKey, cf, limit);
            break;
          } catch (final TiKVException e) {
            backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          }
        }
      }
      return region;
    }
  }

  private boolean notEndOfScan() {
    return limit > 0
        && !(lastBatch
            && (index >= currentCache.size()
                || Key.toRawKey(currentCache.get(index).getKey()).compareTo(endKey) >= 0));
  }

  @Override
  public boolean hasNext() {
    if (isCacheDrained() && cacheLoadFails()) {
      endOfScan = true;
      return false;
    }
    return notEndOfScan();
  }
}
