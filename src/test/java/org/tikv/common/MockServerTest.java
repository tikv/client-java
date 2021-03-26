package org.tikv.common;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.junit.Before;
import org.tikv.common.TiConfiguration.KVMode;
import org.tikv.common.region.TiRegion;
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

    region =
        new TiRegion(
            r,
            r.getPeers(0),
            session.getConf().getIsolationLevel(),
            session.getConf().getCommandPriority(),
            KVMode.TXN,
            new InternalReplicaSelector(TiConfiguration.ReplicaRead.LEADER));
    pdServer.addGetRegionResp(Pdpb.GetRegionResponse.newBuilder().setRegion(r).build());
    server = new KVMockServer();
    port = server.start(region);
  }
}
