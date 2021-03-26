package org.tikv.txn;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.tikv.common.ReplicaSelector;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Metapb;

public class ReplicaReadTest extends TXNTest {
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

  @Test
  public void replicaSelectorTest() {
    TiConfiguration conf = TiConfiguration.createDefault();

    conf.setReplicaSelector(
        new ReplicaSelector() {
          @Override
          public List<Metapb.Peer> select(
              Metapb.Peer leader, List<Metapb.Peer> followers, List<Metapb.Peer> learners) {
            List<Metapb.Peer> list = new ArrayList<>();
            list.addAll(followers);
            list.addAll(learners);
            list.add(leader);
            return list;
          }
        });
    session = TiSession.create(conf);

    putKV(key, value);
    ByteString v = session.createSnapshot().get(ByteString.copyFromUtf8(key));
    Assert.assertEquals(value, v.toStringUtf8());
  }

  private void doTest(TiConfiguration.ReplicaRead replicaRead) {
    TiConfiguration conf = TiConfiguration.createDefault();
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
