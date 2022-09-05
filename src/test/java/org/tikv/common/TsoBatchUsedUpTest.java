package org.tikv.common;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.kvproto.Errorpb.Error;
import org.tikv.raw.RawKVClient;

public class TsoBatchUsedUpTest extends MockThreeStoresTest {
  RawKVClient createClient() {
    return session.createRawClient();
  }

  @Test
  public void testTsoBatchUsedUp() {
    ByteString key = ByteString.copyFromUtf8("tso");
    servers.get(0).putError("tso", () -> Error.newBuilder().setMessage("TsoBatchUsedUp"));
    try (RawKVClient client = createClient()) {
      try {
        client.put(key, ByteString.EMPTY);
        Assert.fail();
      } catch (Exception ignore) {
      }
      pdServers.get(0).addGetRegionListener(request -> null);
      // Will not clean region cache
      Assert.assertNotNull(session.getRegionManager().getRegionByKey(key));
    }
  }
}
