package org.tikv.integration;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class ReplicaReadTest extends IntegrationTest {
  private TiSession session;
  private String key;
  private String value;

  @Test
  public void leaderReadTest() {
    doTest(TiConfiguration.ReplicaRead.LEADER);
  }

  // ci only has one TiKV instance
  @Ignore
  public void followerReadTest() {
    doTest(TiConfiguration.ReplicaRead.FOLLOWER);
  }

  @Test
  public void leadAndFollowerReadTest() {
    doTest(TiConfiguration.ReplicaRead.LEADER_AND_FOLLOWER);
  }

  private void doTest(TiConfiguration.ReplicaRead replicaRead) {
    TiConfiguration conf = TiConfiguration.createDefault(getPdAddr());
    conf.setReplicaRead(replicaRead);
    session = TiSession.create(conf);

    putKV(key, value);
    ByteString v = session.createSnapshot().get(ByteString.copyFromUtf8(key));
    Assert.assertEquals(value, v.toStringUtf8());
  }

  @Before
  public void setUp() {
    super.setUp();
    key = genRandomKey(64);
    value = "v0";
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
    super.tearDown();
  }
}
