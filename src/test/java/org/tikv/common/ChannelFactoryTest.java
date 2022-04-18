package org.tikv.common;

import static org.junit.Assert.assertTrue;

import java.io.File;
import org.junit.Test;
import org.tikv.common.util.ChannelFactory;

public class ChannelFactoryTest {
  @Test
  public void testTlsReload() {
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
  }
}
