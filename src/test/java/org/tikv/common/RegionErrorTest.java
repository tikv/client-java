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

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.TiConfiguration.ApiVersion;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.raw.RawKVClient;

public class RegionErrorTest extends MockThreeStoresTest {
  private RawKVClient createClient() {
    return TiSession.create(session.getConf().setApiVersion(ApiVersion.V2)).createRawClient();
  }

  private ByteString encode(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesCodec.writeBytes(cdo, key.toByteArray());
    return cdo.toByteString();
  }

  @Test
  public void testOnEpochNotMatch() {
    try (RawKVClient client = createClient()) {
      ByteString key = ByteString.copyFromUtf8("test-epoch-not-match");
      ByteString value = ByteString.copyFromUtf8("value");

      ByteString requestKey = client.getSession().getConf().buildRequestKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));

      // Increase the region epoch for the cluster
      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              Metapb.Region.newBuilder()
                  .mergeFrom(this.region.getMeta())
                  .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
                  .build(),
              this.region.getLeader(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));

      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
      }

      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }
}
