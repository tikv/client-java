/*
 * Copyright 2022 TiKV Project Authors.
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

package org.tikv.common;

import static org.tikv.common.GrpcUtils.encodeKey;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.tikv.common.apiversion.RequestKeyV1TxnCodec;
import org.tikv.common.key.Key;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.region.TiStoreType;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;

public class MockRegionManager extends RegionManager {

  private final Map<KeyRange, TiRegion> mockRegionMap;

  private static TiRegion region(long id, KeyRange range) {
    RequestKeyV1TxnCodec v1 = new RequestKeyV1TxnCodec();

    TiConfiguration configuration = new TiConfiguration();
    configuration.setIsolationLevel(IsolationLevel.RC);
    configuration.setCommandPriority(CommandPri.Low);
    Region r =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
            .setId(id)
            .setStartKey(encodeKey(range.getStart().toByteArray()))
            .setEndKey(encodeKey(range.getEnd().toByteArray()))
            .addPeers(Peer.getDefaultInstance())
            .build();

    List<Metapb.Store> s = ImmutableList.of(Metapb.Store.newBuilder().setId(id).build());

    return new TiRegion(
        configuration,
        v1.decodeRegion(r),
        null,
        r.getPeersList(),
        s.stream().map(TiStore::new).collect(Collectors.toList()));
  }

  public MockRegionManager(List<KeyRange> ranges) {
    super(null, null);
    mockRegionMap =
        ranges.stream().collect(Collectors.toMap(kr -> kr, kr -> region(ranges.indexOf(kr), kr)));
  }

  @Override
  public Pair<TiRegion, TiStore> getRegionStorePairByKey(ByteString key, TiStoreType storeType) {
    for (Map.Entry<KeyRange, TiRegion> entry : mockRegionMap.entrySet()) {
      KeyRange range = entry.getKey();
      if (KeyRangeUtils.makeRange(range.getStart(), range.getEnd()).contains(Key.toRawKey(key))) {
        TiRegion region = entry.getValue();
        return Pair.create(
            region, new TiStore(Metapb.Store.newBuilder().setId(region.getId()).build()));
      }
    }
    return null;
  }
}
