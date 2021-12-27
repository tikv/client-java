/*
 * Copyright 2021 TiKV Project Authors.
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

package org.tikv.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Test;
import org.tikv.BaseRawKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class MetricsTest extends BaseRawKVTest {
  private final List<TiSession> sessionList = new ArrayList<>();

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
    TiConfiguration conf = createTiConfiguration();
    conf.setMetricsEnable(true);
    TiSession session = TiSession.create(conf);
    sessionList.add(session);
    RawKVClient client = session.createRawClient();
    client.put(ByteString.copyFromUtf8("k"), ByteString.copyFromUtf8("v"));
    Optional<ByteString> result = client.get(ByteString.copyFromUtf8("k"));
    assertTrue(result.isPresent());
    assertEquals("v", result.get().toStringUtf8());
    client.close();
    session.close();
  }

  @Test
  public void twoTiSession() throws Exception {
    TiConfiguration conf = createTiConfiguration();
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
    assertEquals("v2", result.get().toStringUtf8());

    client2.close();
    session2.close();
  }

  @Test
  public void twoTiSessionWithDifferentPort() {
    TiConfiguration conf1 = createTiConfiguration();
    conf1.setMetricsEnable(true);
    conf1.setMetricsPort(12345);
    TiSession session1 = TiSession.create(conf1);
    sessionList.add(session1);

    TiConfiguration conf2 = createTiConfiguration();
    conf2.setMetricsEnable(true);
    conf2.setMetricsPort(54321);
    try {
      TiSession.create(conf2);
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Do dot support multiple tikv.metrics.port, which are 54321 and 12345", e.getMessage());
    }
  }
}
