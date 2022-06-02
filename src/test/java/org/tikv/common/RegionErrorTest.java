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
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.raw.RawKVClient;

public class RegionErrorTest extends MockThreeStoresTest {
  @Before
  public void init() throws Exception {
    upgradeToV2Cluster();
  }

  private RawKVClient createClient() {
    return session.createRawClient();
  }

  @Test
  public void testOnEpochNotMatch() {
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-epoch-not-match");
      ByteString value = ByteString.copyFromUtf8("value");

      ByteString requestKey = client.getSession().getPDClient().getCodec().encodeKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));

      Metapb.Region newMeta =
          Metapb.Region.newBuilder()
              .mergeFrom(this.region.getMeta())
              .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
              .setStartKey(PDClientV2MockTest.encode(requestKey))
              .setEndKey(PDClientV2MockTest.encode(requestKey.concat(ByteString.copyFromUtf8("0"))))
              .build();

      // Increase the region epoch for the cluster,
      // this will cause the cluster return an EpochNotMatch region error.
      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              newMeta,
              this.region.getLeader(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));

      // Update the region of each server
      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
      }

      // Forbid the client get region from PD leader.
      leader.addGetRegionListener(request -> null);

      // The get should success since the region cache
      // will be updated the currentRegions of `EpochNotMatch` error.
      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }
}
