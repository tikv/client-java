/*
 * Copyright 2023 TiKV Project Authors.
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

package org.tikv.common.util;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;
import org.tikv.common.PDMockServerTest;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;

public class PairTest extends PDMockServerTest {

  @Test
  public void testPair() {
    Metapb.Region r =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
            .setId(233)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addPeers(Peer.getDefaultInstance())
            .build();
    List<Metapb.Store> s =
        ImmutableList.of(
            Metapb.Store.newBuilder()
                .setAddress(LOCAL_ADDR + ":" + 4000)
                .setVersion("5.0.0")
                .setId(1)
                .build());

    TiRegion region =
        new TiRegion(
            session.getConf(),
            r,
            r.getPeers(0),
            r.getPeersList(),
            s.stream().map(TiStore::new).collect(Collectors.toList()));
    TiStore store = new TiStore(s.get(0));

    Map<Pair<TiRegion, TiStore>, List<ByteString>> groupKeyMap = new HashMap<>();

    for (int i = 0; i < 10; i++) {
      Pair<TiRegion, TiStore> pair = Pair.create(region, store);
      groupKeyMap
          .computeIfAbsent(pair, e -> new ArrayList<>())
          .add(ByteString.copyFromUtf8("test"));
    }
    Pair<TiRegion, TiStore> pair = Pair.create(region, store);
    assert (groupKeyMap.get(pair).size() == 10);
  }
}
