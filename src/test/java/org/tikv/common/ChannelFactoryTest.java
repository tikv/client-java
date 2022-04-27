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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.grpc.ManagedChannel;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ChannelFactory.CertContext;

public class ChannelFactoryTest {
  private final AtomicLong ts = new AtomicLong(System.currentTimeMillis());
  private final String tlsPath = "src/test/resources/tls/";
  private final String caPath = "ca.crt";
  private final String clientCertPath = "client.crt";
  private final String clientKeyPath = "client.pem";

  private ChannelFactory createFactory() {
    int v = 1024;
    return new ChannelFactory(v, v, v, v, 5, tlsPath, caPath, clientCertPath, clientKeyPath);
  }

  private CertContext createCertContext() {
    return new ChannelFactory.OpenSslContext(tlsPath, caPath, clientCertPath, clientKeyPath);
  }

  private void touchCert() {
    ts.addAndGet(100_000_000);
    assertTrue(new File(tlsPath + caPath).setLastModified(ts.get()));
  }

  @Test
  public void testSingleThreadTlsReload() throws Exception {
    CertContext context = createCertContext();
    SslContextBuilder a = context.createSslContextBuilder();
    assertNotNull(a);
    assertTrue(context.isModified());

    assertFalse(context.isModified());
    SslContextBuilder b = context.createSslContextBuilder();
    assertNotEquals(b, a);

    touchCert();

    Thread.sleep(100);
    assertTrue(context.isModified());
    SslContextBuilder c = context.createSslContextBuilder();
    assertNotEquals(c, b);
    assertNotEquals(c, a);
  }

  @Test
  public void testMultiThreadTlsReload() throws InterruptedException {
    ChannelFactory factory = createFactory();
    HostMapping hostMapping = uri -> uri;

    int taskCount = Runtime.getRuntime().availableProcessors() * 2;
    List<Thread> tasks = new ArrayList<>(taskCount);
    for (int i = 0; i < taskCount; i++) {
      Thread t =
          new Thread(
              () -> {
                for (int j = 0; j < 100; j++) {
                  String addr = "127.0.0.1:237" + (j % 2 == 0 ? 9 : 8);
                  ManagedChannel c = factory.getChannel(addr, hostMapping);
                  assertNotNull(c);
                  c.shutdownNow();
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException ignore) {
                  }
                }
              });
      t.start();
      tasks.add(t);
    }
    Thread reactor =
        new Thread(
            () -> {
              for (int i = 0; i < 100; i++) {
                touchCert();
                try {
                  Thread.sleep(100);
                } catch (InterruptedException ignore) {
                }
              }
            });
    reactor.start();

    for (Thread t : tasks) {
      t.join();
    }
    reactor.join();

    factory.close();
    assertTrue(factory.connPool.isEmpty());
  }
}
