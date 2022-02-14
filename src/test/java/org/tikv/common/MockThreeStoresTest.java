package org.tikv.common;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Pdpb;

public class MockThreeStoresTest extends PDMockServerTest {

  public TiRegion region;
  public ArrayList<KVMockServer> servers = new ArrayList<>();

  @Before
  @Override
  public void setup() throws IOException {
    super.setup();

    int basePort = 0xabcd;

    Metapb.Region region =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
            .setId(0xff)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addPeers(Metapb.Peer.newBuilder().setId(0x1).setStoreId(0x1))
            .addPeers(Metapb.Peer.newBuilder().setId(0x2).setStoreId(0x2))
            .addPeers(Metapb.Peer.newBuilder().setId(0x3).setStoreId(0x3))
            .build();

    List<Metapb.Store> stores =
        ImmutableList.of(
            Metapb.Store.newBuilder()
                .setAddress("127.0.0.1:" + basePort)
                .setVersion("5.0.0")
                .setId(0x1)
                .build(),
            Metapb.Store.newBuilder()
                .setAddress("127.0.0.1:" + (basePort + 1))
                .setVersion("5.0.0")
                .setVersion("5.0.0")
                .setId(0x2)
                .build(),
            Metapb.Store.newBuilder()
                .setAddress("127.0.0.1:" + (basePort + 2))
                .setVersion("5.0.0")
                .setId(0x3)
                .build()
        );

    for (PDMockServer server : pdServers) {
      server.addGetRegionResp(Pdpb.GetRegionResponse.newBuilder().setRegion(region).build());
      for (Metapb.Store store : stores) {
        server.addGetStoreResp(Pdpb.GetStoreResponse.newBuilder().setStore(store).build());
      }
    }

    this.region =
        new TiRegion(
            session.getConf(),
            region,
            region.getPeers(0),
            region.getPeersList(),
            stores.stream().map(TiStore::new).collect(Collectors.toList()));
    for (int i = 0; i < 3; i++) {
      KVMockServer server = new KVMockServer();
      server.start(this.region, basePort + i);
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
