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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ChannelFactory.CertContext;

public class ChannelFactoryTest {
  private final AtomicLong ts = new AtomicLong(System.currentTimeMillis());
  private final String tlsPath = "src/test/resources/tls/";
  private final String caPath = tlsPath + "ca.crt";

  private CertContext createCertContext() {
    String clientCertPath = tlsPath + "client.crt";
    String clientKeyPath = tlsPath + "client.pem";
    return new ChannelFactory.OpenSslContext(caPath, clientCertPath, clientKeyPath);
  }

  private void touchCert() {
    ts.addAndGet(100_000_000);
    assertTrue(new File(caPath).setLastModified(ts.get()));
  }

  @Test
  public void testSingleThreadTlsReload() throws InterruptedException {
    CertContext certContext = createCertContext();

    // The first time reloading.
    certContext.reload(null);
    SslContextBuilder a = certContext.getSslContextBuilder();
    assertNotNull(a);
    assertEquals(a, certContext.getSslContextBuilder());

    touchCert();

    // The second time reloading.
    certContext.reload(null);
    SslContextBuilder b = certContext.getSslContextBuilder();
    assertNotNull(b);
    // Should get a new SslContextBuilder.
    assertNotEquals(a, b);

    Map<String, String> map = new HashMap<>(ImmutableMap.of("key", "value"));
    certContext.reload(map);
    assertFalse(map.isEmpty());

    touchCert();

    certContext.reload(map);
    assertTrue(map.isEmpty());
  }

  @Test
  public void testMultiThreadTlsReload() throws InterruptedException {
    CertContext certContext = createCertContext();
    Map<Integer, SslContextBuilder> map = new ConcurrentHashMap<>();
    List<Thread> tasks = new ArrayList<>(8);
    for (int i = 0; i < 8; i++) {
      int threadId = i;
      Thread t =
          new Thread(
              () -> {
                certContext.reload(null);
                SslContextBuilder builder = certContext.getSslContextBuilder();
                map.put(threadId, builder);
                assertNotNull(builder);
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
              });
      t.start();
      tasks.add(t);
    }

    for (Thread t : tasks) {
      t.join();
    }

    // All the threads should get the same SslContextBuilder.
    SslContextBuilder unit = map.get(0);
    for (SslContextBuilder v : map.values()) {
      assertEquals(unit, v);
    }
  }
}
