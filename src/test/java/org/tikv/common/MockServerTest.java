/*
 * Copyright 2021 TiKV Project Authors.
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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Pdpb;

public class MockServerTest extends PDMockServerTest {
  public KVMockServer server;
  public int port;
  public TiRegion region;

  @Before
  @Override
  public void setup() throws IOException {
    super.setup();

    Metapb.Region r =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
            .setId(233)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addPeers(Metapb.Peer.newBuilder().setId(11).setStoreId(13))
            .build();

    List<Metapb.Store> s =
        ImmutableList.of(
            Metapb.Store.newBuilder()
                .setAddress("localhost:1234")
                .setVersion("5.0.0")
                .setId(13)
                .build());

    region =
        new TiRegion(
            session.getConf(),
            r,
            r.getPeers(0),
            r.getPeersList(),
            s.stream().map(TiStore::new).collect(Collectors.toList()));
    leader.addGetRegionListener(
        request -> Pdpb.GetRegionResponse.newBuilder().setRegion(r).build());
    for (Metapb.Store store : s) {
      leader.addGetStoreListener(
          (request) -> Pdpb.GetStoreResponse.newBuilder().setStore(store).build());
    }
    server = new KVMockServer();
    port = server.start(region);
  }
}
