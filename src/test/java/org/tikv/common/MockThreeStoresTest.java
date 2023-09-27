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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Pdpb;
import org.tikv.kvproto.Pdpb.GetAllStoresResponse;

public class MockThreeStoresTest extends PDMockServerTest {

  protected TiRegion region;
  protected TiRegion firstRegion;
  protected List<KVMockServer> servers = new ArrayList<>();
  protected List<Metapb.Store> stores;

  @Before
  @Override
  public void setup() throws IOException {
    super.setup();

    int[] ports = new int[3];
    for (int i = 0; i < ports.length; i++) {
      try (ServerSocket s = new ServerSocket(0)) {
        ports[i] = s.getLocalPort();
      }
    }

    ImmutableList<Metapb.Peer> peers =
        ImmutableList.of(
            Metapb.Peer.newBuilder().setId(0x1).setStoreId(0x1).build(),
            Metapb.Peer.newBuilder().setId(0x2).setStoreId(0x2).build(),
            Metapb.Peer.newBuilder().setId(0x3).setStoreId(0x3).build());

    Metapb.Region region =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
            .setId(0xff)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addAllPeers(peers)
            .build();

    stores =
        ImmutableList.of(
            Metapb.Store.newBuilder()
                .setAddress("127.0.0.1:" + ports[0])
                .setVersion(Version.API_V2)
                .setId(0x1)
                .build(),
            Metapb.Store.newBuilder()
                .setAddress("127.0.0.1:" + ports[1])
                .setVersion(Version.API_V2)
                .setId(0x2)
                .build(),
            Metapb.Store.newBuilder()
                .setAddress("127.0.0.1:" + ports[2])
                .setVersion(Version.API_V2)
                .setId(0x3)
                .build());

    for (PDMockServer server : pdServers) {
      server.addGetRegionListener(
          request ->
              Pdpb.GetRegionResponse.newBuilder()
                  .setLeader(peers.get(0))
                  .setRegion(region)
                  .build());
      server.addGetStoreListener(
          (request) -> {
            int i = (int) request.getStoreId() - 1;
            return Pdpb.GetStoreResponse.newBuilder().setStore(stores.get(i)).build();
          });
      server.addGetAllStoresListener(
          request -> {
            return GetAllStoresResponse.newBuilder().addAllStores(stores).build();
          });
    }

    this.region =
        new TiRegion(
            session.getConf(),
            region,
            region.getPeers(0),
            region.getPeersList(),
            stores.stream().map(TiStore::new).collect(Collectors.toList()));
    this.firstRegion =
        new TiRegion(
            session.getConf(),
            region,
            region.getPeers(0),
            region.getPeersList(),
            stores.stream().map(TiStore::new).collect(Collectors.toList()));
    for (int port : ports) {
      KVMockServer server = new KVMockServer();
      server.start(this.region, port);
      servers.add(server);
    }
  }

  public void put(ByteString key, ByteString value) {
    for (KVMockServer server : servers) {
      server.put(key, value);
    }
  }

  public void remove(ByteString key, ByteString value) {
    for (KVMockServer server : servers) {
      server.remove(key);
    }
  }

  @After
  public void tearDown() {
    for (KVMockServer server : servers) {
      server.stop();
    }
  }
}
