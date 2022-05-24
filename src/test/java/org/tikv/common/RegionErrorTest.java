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

      Assert.assertEquals(Optional.of(value), client.get(key));

      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
      }

      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }
}
