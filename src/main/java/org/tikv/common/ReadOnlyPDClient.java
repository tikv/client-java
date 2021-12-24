/*
 * Copyright 2017 TiKV Project Authors.
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

package org.tikv.common;

import com.google.protobuf.ByteString;
import java.util.List;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.Pdpb;

/** Readonly PD client including only reading related interface Supposed for TiDB-like use cases */
public interface ReadOnlyPDClient {
  /**
   * Get Timestamp from Placement Driver
   *
   * @return a timestamp object
   */
  TiTimestamp getTimestamp(BackOffer backOffer);

  /**
   * Get Region from PD by key specified
   *
   * @param key key in bytes for locating a region
   * @return the region whose startKey and endKey range covers the given key
   */
  Pair<Metapb.Region, Metapb.Peer> getRegionByKey(BackOffer backOffer, ByteString key);

  /**
   * Get Region by Region Id
   *
   * @param id Region Id
   * @return the region corresponding to the given Id
   */
  Pair<Metapb.Region, Metapb.Peer> getRegionByID(BackOffer backOffer, long id);

  List<Pdpb.Region> scanRegions(
      BackOffer backOffer, ByteString startKey, ByteString endKey, int limit);

  HostMapping getHostMapping();

  /**
   * Get Store by StoreId
   *
   * @param storeId StoreId
   * @return the Store corresponding to the given Id
   */
  Store getStore(BackOffer backOffer, long storeId);

  List<Store> getAllStores(BackOffer backOffer);

  TiConfiguration.ReplicaRead getReplicaRead();
}
