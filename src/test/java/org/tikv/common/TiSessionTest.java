package org.tikv.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.BaseRawKVTest;
import org.tikv.common.region.TiRegion;
import org.tikv.raw.RawKVClient;

public class TiSessionTest extends BaseRawKVTest {
  private static final Logger logger = LoggerFactory.getLogger(TiSessionTest.class);
  private TiSession session;

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void closeWithRunningTaskTest() throws Exception {
    doCloseWithRunningTaskTest(true, 0);
  }

  @Ignore
  public void closeAwaitTerminationWithRunningTaskTest() throws Exception {
    doCloseWithRunningTaskTest(false, 10000);
  }

  private void doCloseWithRunningTaskTest(boolean now, long timeoutMS) throws Exception {
    TiConfiguration conf = createTiConfiguration();
    session = TiSession.create(conf);

    ExecutorService executorService = session.getThreadPoolForBatchGet();
    AtomicReference<InterruptedException> interruptedException = new AtomicReference<>();
    executorService.submit(
        () -> {
          int i = 1;
          while (true) {
            i = i + 1;
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              interruptedException.set(e);
              break;
            }
          }
        });

    Thread.sleep(2000);

    long startMS = System.currentTimeMillis();
    if (now) {
      session.close();
      Thread.sleep(1000);
      assertNotNull(interruptedException.get());
    } else {
      session.closeAwaitTermination(timeoutMS);
      assertNotNull(interruptedException.get());
      assertTrue(System.currentTimeMillis() - startMS >= timeoutMS);
    }
  }

  @Test
  public void closeTest() throws Exception {
    doCloseTest(true, 0);
  }

  @Test
  public void closeAwaitTerminationTest() throws Exception {
    doCloseTest(false, 10000);
  }

  private void doCloseTest(boolean now, long timeoutMS) throws Exception {
    TiConfiguration conf = createTiConfiguration();
    session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    // test getRegionByKey
    ByteString key = ByteString.copyFromUtf8("key");
    ByteString value = ByteString.copyFromUtf8("value");
    TiRegion region = session.getRegionManager().getRegionByKey(key);
    assertNotNull(region);

    // test RawKVClient
    client.put(key, value);
    List<ByteString> keys = new ArrayList<>();
    keys.add(key);
    client.batchGet(keys);

    // close TiSession
    if (now) {
      session.close();
    } else {
      session.closeAwaitTermination(timeoutMS);
    }

    // test getRegionByKey
    try {
      session.getRegionManager().getRegionByKey(key);
      fail();
    } catch (RuntimeException e) {
      assertEquals("this TiSession is closed!", e.getMessage());
    }

    // test RawKVClient
    try {
      client.batchGet(keys);
      fail();
    } catch (RejectedExecutionException e) {
      assertTrue(e.getMessage().contains("rejected from java.util.concurrent.ThreadPoolExecutor"));
    }
  }

  @Test
  public void warmUpTest() throws Exception {
    TiConfiguration conf = createTiConfiguration();
    conf.setWarmUpEnable(true);
    long t0 = doTest(conf);
    conf.setWarmUpEnable(false);
    long t1 = doTest(conf);
    assertTrue(t0 < t1);
  }

  private long doTest(TiConfiguration conf) throws Exception {
    session = TiSession.create(conf);
    long start = System.currentTimeMillis();
    try (RawKVClient client = session.createRawClient()) {
      client.get(ByteString.EMPTY);
    }
    long end = System.currentTimeMillis();
    logger.info(
        "[warm up "
            + (conf.isWarmUpEnable() ? "enabled" : "disabled")
            + "] duration "
            + (end - start)
            + "ms");
    session.close();
    return end - start;
  }
}
