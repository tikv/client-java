package org.tikv.common.importer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class SwitchTiKVModeTest {
  private TiSession session;

  @Before
  public void setup() {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void swithTiKVModeTest() throws InterruptedException {
    SwitchTiKVModeClient switchTiKVModeClient = session.getSwitchTiKVModeClient();
    switchTiKVModeClient.keepTiKVToImportMode();
    Thread.sleep(6000);
    switchTiKVModeClient.stopKeepTiKVToImportMode();
    switchTiKVModeClient.switchTiKVToNormalMode();
  }
}
