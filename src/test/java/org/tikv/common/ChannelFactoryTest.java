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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.tikv.common.util.ChannelFactory;

public class ChannelFactoryTest {
  @Test
  public void testTlsReload() throws InterruptedException {
    final int v = 1024;
    String tlsPath = "src/test/resources/tls/";
    String caPath = tlsPath + "ca.crt";
    String clientCertPath = tlsPath + "client.crt";
    String clientKeyPath = tlsPath + "client.pem";
    ChannelFactory factory = new ChannelFactory(v, v, v, v, caPath, clientCertPath, clientKeyPath);
    HostMapping mapping = uri -> uri;

    factory.getChannel("127.0.0.1:2379", mapping);

    assertTrue(new File(clientKeyPath).setLastModified(System.currentTimeMillis()));

    assertTrue(factory.reloadSslContext());

    factory.close();

    // Test getChannel concurrently
    factory = new ChannelFactory(v, v, v, v, caPath, clientCertPath, clientKeyPath);

    List<Thread> tasks = new ArrayList<>(8);
    for (int i = 0; i < 8; i++) {
      ChannelFactory finalFactory = factory;
      Thread t = new Thread(() -> finalFactory.getChannel("127.0.0.1:2379", mapping));
      t.start();
      tasks.add(t);
    }

    for (Thread t : tasks) {
      t.join();
    }
  }
}
