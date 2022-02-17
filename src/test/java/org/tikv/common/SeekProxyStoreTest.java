package org.tikv.common;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.KVMockServer.State;
import org.tikv.raw.RawKVClient;

public class SeekProxyStoreTest extends MockThreeStoresTest {

  private RawKVClient createClient() {
    RawKVClient client = session.createRawClient();
    return client;
  }

  @Test
  public void testSeekProxyStore() {
    RawKVClient client = createClient();
    ByteString key = ByteString.copyFromUtf8("key");
    ByteString value = ByteString.copyFromUtf8("value");
    put(key, value);

    client.put(key, value);
    Assert.assertEquals(value, client.get(key).get());
    servers.get(0).setState(State.Fail);
    Assert.assertEquals(value, client.get(key).get());
  }
}
