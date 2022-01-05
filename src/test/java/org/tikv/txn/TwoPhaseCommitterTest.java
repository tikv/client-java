package org.tikv.txn;

import static org.junit.Assert.fail;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseTxnKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class TwoPhaseCommitterTest extends BaseTxnKVTest {
  private static final int WRITE_BUFFER_SIZE = 32 * 1024;
  private static final int TXN_COMMIT_BATCH_SIZE = 768 * 1024;
  private static final long DEFAULT_BATCH_WRITE_LOCK_TTL = 3600000;

  private TiSession session;

  @Before
  public void setUp() {
    TiConfiguration conf = createTiConfiguration();
    try {
      session = TiSession.create(conf);
    } catch (Exception e) {
      fail("TiDB cluster may not be present");
    }
  }

  @Test
  public void autoClosableTest() throws Exception {
    long startTS = session.getTimestamp().getVersion();
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            WRITE_BUFFER_SIZE,
            new ThreadFactoryBuilder().setNameFormat("2pc-pool-%d").setDaemon(true).build());
    Assert.assertFalse(executorService.isShutdown());
    try (TwoPhaseCommitter twoPhaseCommitter =
        new TwoPhaseCommitter(
            session,
            startTS,
            DEFAULT_BATCH_WRITE_LOCK_TTL,
            TXN_COMMIT_BATCH_SIZE,
            TXN_COMMIT_BATCH_SIZE,
            WRITE_BUFFER_SIZE,
            1,
            true,
            3,
            executorService)) {}
    Assert.assertTrue(executorService.isShutdown());
  }
}
