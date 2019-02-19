package org.tikv.txn;

import org.junit.Before;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class TxnKVClientTest {
  private static final String DEFAULT_PD_ADDRESS = "127.0.0.1:2379";
  private static boolean initialized;
  private static TxnKVClient client = null;

  @Before
  public void setUp() {
    try {
      TiSession session = TiSession.create(TiConfiguration.createDefault(DEFAULT_PD_ADDRESS));
      initialized = false;
      if (client == null) {
        client = session.createTxnClient();
      }
      initialized = true;
    } catch (Exception e) {
      System.out.println("Cannot initialize txn client. Test skipped.");
      e.printStackTrace();
    }
  }

  @Test
  public void test() {
    if (!initialized) return;
    try {
      Thread.sleep(60 * 1000);
    } catch (InterruptedException e) {
      // ignore
    }
  }
}
