/*
 * Copyright 2017 TiKV Project Authors.
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

import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Pdpb;
import org.tikv.kvproto.Pdpb.GetRegionResponse;
import org.tikv.kvproto.Pdpb.Region;
import org.tikv.kvproto.Pdpb.ScanRegionsResponse;

public class PDClientV2MockTest extends PDClientMockTest {
  @Before
  public void init() throws Exception {
    upgradeToV2Cluster();
  }

  private PDClient createClient() {
    return session.getPDClient();
  }


  public static ByteString encode(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesCodec.writeBytes(cdo, key.toByteArray());
    return cdo.toByteString();
  }

  private GetRegionResponse makeGetRegionResponse(String start, String end) {
    return GrpcUtils.makeGetRegionResponse(leader.getClusterId(), makeRegion(start, end));
  }

  private Metapb.Region makeRegion(String start, String end) {
    return GrpcUtils.makeRegion(
        1,
        encode(session.getConf().buildRequestKey(ByteString.copyFromUtf8(start))),
        encode(session.getConf().buildRequestKey(ByteString.copyFromUtf8(end), true)),
        GrpcUtils.makeRegionEpoch(2, 3),
        GrpcUtils.makePeer(1, 10),
        GrpcUtils.makePeer(2, 20));
  }

  @Test
  public void testGetRegionById() throws Exception {
    String start = "getRegionById";
    String end = "getRegionByIdEnd";
    leader.addGetRegionByIDListener(
        request -> makeGetRegionResponse(start, end));
    try (PDClient client = createClient()) {
      Metapb.Region r = client.getRegionByID(ConcreteBackOffer.newRawKVBackOff(), 1).first;
      Assert.assertEquals(start, r.getStartKey().toStringUtf8());
      Assert.assertEquals(end, r.getEndKey().toStringUtf8());
    }

    leader.addGetRegionByIDListener(request -> makeGetRegionResponse(start, ""));
    try (PDClient client = createClient()) {
      Metapb.Region r = client.getRegionByID(ConcreteBackOffer.newRawKVBackOff(), 1).first;
      Assert.assertEquals(start, r.getStartKey().toStringUtf8());
      Assert.assertEquals("", r.getEndKey().toStringUtf8());
    }
  }

  @Test
  public void testScanRegions() throws Exception {
    String start = "scanRegions";
    String end = "scanRegionsEnd";

    leader.addScanRegionsListener(request ->
        ScanRegionsResponse.newBuilder()
            .addRegions(Pdpb.Region.newBuilder().setRegion(makeRegion(start, end)).build())
            .build());

    try (PDClient client = createClient()) {
      List<Region> regions = client.
          scanRegions(ConcreteBackOffer.newRawKVBackOff(), ByteString.EMPTY, ByteString.EMPTY,
              1);

      for (Region r : regions) {
        Assert.assertEquals(start, r.getRegion().getStartKey().toStringUtf8());
        Assert.assertEquals(end, r.getRegion().getEndKey().toStringUtf8());
      }
    }
  }
}
