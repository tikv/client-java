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
  public void setUp() throws IOException {
    super.setUp();

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
    pdServer.addGetRegionResp(Pdpb.GetRegionResponse.newBuilder().setRegion(r).build());
    for (Metapb.Store store : s) {
      pdServer.addGetStoreResp(Pdpb.GetStoreResponse.newBuilder().setStore(store).build());
    }
    server = new KVMockServer();
    port = server.start(region);
  }
}
