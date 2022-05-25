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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb;

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

  @Test
  public void testGetRegionById() throws Exception {
    ByteString start = ByteString.copyFromUtf8("regionById");
    ByteString end = ByteString.copyFromUtf8("regionById0");
    leader.addGetRegionByIDListener(
        request ->
            GrpcUtils.makeGetRegionResponse(
                leader.getClusterId(),
                GrpcUtils.makeRegion(
                    1,
                    encode(session.getConf().buildRequestKey(start)),
                    encode(session.getConf().buildRequestKey(end)),
                    GrpcUtils.makeRegionEpoch(2, 3),
                    GrpcUtils.makePeer(1, 10),
                    GrpcUtils.makePeer(2, 20))));
    try (PDClient client = createClient()) {
      Metapb.Region r = client.getRegionByID(ConcreteBackOffer.newRawKVBackOff(), 1).first;
      Assert.assertEquals(start, r.getStartKey());
      Assert.assertEquals(end, r.getEndKey());
    }

    leader.addGetRegionByIDListener(
        request ->
            GrpcUtils.makeGetRegionResponse(
                leader.getClusterId(),
                GrpcUtils.makeRegion(
                    1,
                    encode(session.getConf().buildRequestKey(start)),
                    encode(session.getConf().getEndKey()),
                    GrpcUtils.makeRegionEpoch(2, 3),
                    GrpcUtils.makePeer(1, 10),
                    GrpcUtils.makePeer(2, 20))));
    try (PDClient client = createClient()) {
      Metapb.Region r = client.getRegionByID(ConcreteBackOffer.newRawKVBackOff(), 1).first;
      Assert.assertEquals(start, r.getStartKey());
      Assert.assertEquals(ByteString.EMPTY, r.getEndKey());
    }
  }

  @Test
  public void testScanRegions() {
  }
}
