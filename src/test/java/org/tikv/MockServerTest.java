package org.tikv;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Pdpb;
import org.tikv.region.TiRegion;

public class MockServerTest {
  public KVMockServer server;
  public PDMockServer pdServer;
  public static final String LOCAL_ADDR = "127.0.0.1";
  static final long CLUSTER_ID = 1024;
  public int port;
  public TiSession session;
  public TiRegion region;

  @Before
  public void setUp() throws Exception {
    pdServer = new PDMockServer();
    pdServer.start(CLUSTER_ID);
    pdServer.addGetMemberResp(
        GrpcUtils.makeGetMembersResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeMember(1, "http://" + LOCAL_ADDR + ":" + pdServer.port),
            GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (pdServer.port + 1)),
            GrpcUtils.makeMember(2, "http://" + LOCAL_ADDR + ":" + (pdServer.port + 2))));

    Metapb.Region r =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
            .setId(233)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addPeers(Metapb.Peer.newBuilder().setId(11).setStoreId(13))
            .build();

    region =
        new TiRegion(r, r.getPeers(0), Kvrpcpb.IsolationLevel.RC, Kvrpcpb.CommandPri.Low, "KV");
    pdServer.addGetRegionResp(Pdpb.GetRegionResponse.newBuilder().setRegion(r).build());
    server = new KVMockServer();
    port = server.start(region);
    // No PD needed in this test
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:" + pdServer.port);
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @VisibleForTesting
  protected static Coprocessor.KeyRange createByteStringRange(ByteString sKey, ByteString eKey) {
    return Coprocessor.KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }
}
