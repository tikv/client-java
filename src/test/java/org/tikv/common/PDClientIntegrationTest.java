package org.tikv.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PDClientIntegrationTest {
  private TiSession session;

  @Before
  public void setup() {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    conf.setTest(true);
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void testPauseCheck() throws Exception {
    try (PDClient client = session.getPDClient()) {
      PDChecker checker = PDChecker.Merge;
      for (int i = 0; i < 2; i++) {
        client.keepPauseChecker(checker);
        Thread.sleep(1000);
        assertTrue(client.isCheckerPaused(checker));

        client.stopKeepPauseChecker(checker);
        Thread.sleep(1000);

        client.resumeChecker(checker);
        assertFalse(client.isCheckerPaused(checker));
      }
    }
  }
}
