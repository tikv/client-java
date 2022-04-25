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
  private final String caPath = tlsPath + "ca.crt";

  private ChannelFactory createFactory() {
    int v = 1024;
    String clientCertPath = tlsPath + "client.crt";
    String clientKeyPath = tlsPath + "client.pem";
    return new ChannelFactory(v, v, v, v, caPath, clientCertPath, clientKeyPath);
  }

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
  public void testSingleThreadTlsReload() {
    CertContext context = createCertContext();
    SslContextBuilder a = context.createSslContextBuilder();
    assertNotNull(a);
    assertTrue(context.isModified());

    assertFalse(context.isModified());
    SslContextBuilder b = context.createSslContextBuilder();
    assertNotEquals(b, a);

    touchCert();
    assertTrue(context.isModified());
    SslContextBuilder c = context.createSslContextBuilder();
    assertNotEquals(c, b);
    assertNotEquals(c, a);
  }

  @Test
  public void testMultiThreadTlsReload() throws InterruptedException {
    ChannelFactory factory = createFactory();
    HostMapping hostMapping = uri -> uri;

    int taskCount = 16;
    List<Thread> tasks = new ArrayList<>(taskCount);
    for (int i = 0; i < taskCount; i++) {
      Thread t =
          new Thread(
              () -> {
                for (int j = 0; j < 100; j++) {
                  ManagedChannel c = factory.getChannel("127.0.0.1:2379", hostMapping);
                  assertNotNull(c);
                  c.shutdown();
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
  }
}
