package org.tikv.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class MetricsTest {
  private List<TiSession> sessionList = new ArrayList<>();

  @After
  public void tearDown() throws Exception {
    for (TiSession tiSession : sessionList) {
      if (tiSession != null) {
        tiSession.close();
      }
    }
  }

  @Test
  public void oneTiSession() throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    conf.setMetricsEnable(true);
    TiSession session = TiSession.create(conf);
    sessionList.add(session);
    RawKVClient client = session.createRawClient();
    client.put(ByteString.copyFromUtf8("k"), ByteString.copyFromUtf8("v"));
    Optional<ByteString> result = client.get(ByteString.copyFromUtf8("k"));
    assertTrue(result.isPresent());
    assertEquals(result.get().toStringUtf8(), "v");
    client.close();
    session.close();
  }

  /*@Test
  public void twoTiSession() throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    conf.setMetricsEnable(true);

    TiSession session1 = TiSession.create(conf);
    sessionList.add(session1);
    RawKVClient client1 = session1.createRawClient();
    client1.put(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("v1"));

    TiSession session2 = TiSession.create(conf);
    sessionList.add(session2);
    RawKVClient client2 = session2.createRawClient();
    client2.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8("v2"));

    client1.close();
    session1.close();

    Optional<ByteString> result = client2.get(ByteString.copyFromUtf8("k2"));
    assertTrue(result.isPresent());
    assertEquals(result.get().toStringUtf8(), "v2");

    client2.close();
    session2.close();
  }*/

  @Test
  public void twoTiSessionWithDifferentPort() {
    TiConfiguration conf1 = TiConfiguration.createRawDefault();
    conf1.setMetricsEnable(true);
    conf1.setMetricsPort(12345);
    TiSession session1 = TiSession.create(conf1);
    sessionList.add(session1);

    TiConfiguration conf2 = TiConfiguration.createRawDefault();
    conf2.setMetricsEnable(true);
    conf2.setMetricsPort(54321);
    try {
      TiSession.create(conf2);
      assertEquals(1, 2);
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Do dot support multiple tikv.metrics.port, which are 54321 and 12345", e.getMessage());
    }
  }
}
