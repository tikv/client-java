package org.tikv.txn;

import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class KVClientTest {
  private TiSession session;

  @Before
  public void setUp() {
    TiConfiguration conf = TiConfiguration.createDefault();
    conf.setTest(true);
    try {
      session = TiSession.create(conf);
    } catch (Exception e) {
      fail("TiKV cluster may not be present");
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void unsafeDestroyRangeTest() {
    try (KVClient client = session.createKVClient()) {
      client.unsafeDestoryRange(ByteString.EMPTY, ByteString.EMPTY);
    }
  }
}
