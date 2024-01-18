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

package org.tikv.txn;

import static org.junit.Assert.fail;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.BaseTxnKVTest;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.KeyException;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TiSession.class, TxnKVClient.class, TwoPhaseCommitter.class})
@PowerMockIgnore({"javax.net.ssl.*"})
public class TwoPhaseCommitterTest extends BaseTxnKVTest {

  private static final Logger logger = LoggerFactory.getLogger(TwoPhaseCommitterTest.class);
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

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
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

  @Test
  public void prewriteWriteConflictFastFailTest() throws Exception {

    int WRITE_BUFFER_SIZE = 32;
    String primaryKey = RandomStringUtils.randomAlphabetic(3);
    AtomicLong failCount = new AtomicLong();
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            WRITE_BUFFER_SIZE,
            new ThreadFactoryBuilder().setNameFormat("2pc-pool-%d").setDaemon(true).build());
    CountDownLatch latch = new CountDownLatch(2);
    int DEFAULT_BATCH_WRITE_LOCK_TTL = 10000;
    new Thread(
            () -> {
              long startTS = session.getTimestamp().getVersion();
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              try {
                TwoPhaseCommitter twoPhaseCommitter =
                    new TwoPhaseCommitter(
                        session, startTS, DEFAULT_BATCH_WRITE_LOCK_TTL, executorService);
                List<BytePairWrapper> pairs =
                    Arrays.asList(
                        new BytePairWrapper(
                            primaryKey.getBytes(StandardCharsets.UTF_8),
                            primaryKey.getBytes(StandardCharsets.UTF_8)));
                twoPhaseCommitter.prewriteSecondaryKeys(
                    primaryKey.getBytes(StandardCharsets.UTF_8), pairs.iterator(), 20000);
                BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(20000);

                long commitTs = session.getTimestamp().getVersion();

                twoPhaseCommitter.commitPrimaryKey(
                    backOffer, primaryKey.getBytes(StandardCharsets.UTF_8), commitTs);
              } catch (Exception e) {
                logger.error(e.getMessage(), e);
                failCount.incrementAndGet();
              } finally {
                latch.countDown();
              }
            })
        .start();

    Thread.sleep(10);
    new Thread(
            () -> {
              long startTS = session.getTimestamp().getVersion();
              try {
                TwoPhaseCommitter twoPhaseCommitter =
                    new TwoPhaseCommitter(
                        session, startTS, DEFAULT_BATCH_WRITE_LOCK_TTL, executorService);
                List<BytePairWrapper> pairs =
                    Arrays.asList(
                        new BytePairWrapper(
                            primaryKey.getBytes(StandardCharsets.UTF_8),
                            primaryKey.getBytes(StandardCharsets.UTF_8)));
                twoPhaseCommitter.prewriteSecondaryKeys(
                    primaryKey.getBytes(StandardCharsets.UTF_8), pairs.iterator(), 20000);
                BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(20000);

                long commitTs = session.getTimestamp().getVersion();

                twoPhaseCommitter.commitPrimaryKey(
                    backOffer, primaryKey.getBytes(StandardCharsets.UTF_8), commitTs);
              } catch (Exception e) {
                logger.error(e.getMessage(), e);
                failCount.incrementAndGet();
              } finally {
                latch.countDown();
              }
            })
        .start();
    latch.await();
    Assert.assertEquals(1, failCount.get());
  }

  @Test
  public void prewriteWriteConflictLongNoFailTest() throws Exception {

    int WRITE_BUFFER_SIZE = 32;
    String primaryKey = RandomStringUtils.randomAlphabetic(3);
    AtomicLong failCount = new AtomicLong();
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            WRITE_BUFFER_SIZE,
            new ThreadFactoryBuilder().setNameFormat("2pc-pool-%d").setDaemon(true).build());
    CountDownLatch latch = new CountDownLatch(2);
    int DEFAULT_BATCH_WRITE_LOCK_TTL = 10000;

    new Thread(
            () -> {
              long startTS = session.getTimestamp().getVersion();
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }

              try {
                session = PowerMockito.spy(session);
                TxnKVClient kvClient = PowerMockito.spy(session.createTxnClient());
                PowerMockito.when(kvClient, "retryableException", Mockito.any(KeyException.class))
                    .thenReturn(true);
                PowerMockito.doReturn(kvClient).when(session).createTxnClient();

                TwoPhaseCommitter twoPhaseCommitter =
                    new TwoPhaseCommitter(
                        session, startTS, DEFAULT_BATCH_WRITE_LOCK_TTL, executorService);
                List<BytePairWrapper> pairs =
                    Arrays.asList(
                        new BytePairWrapper(
                            primaryKey.getBytes(StandardCharsets.UTF_8),
                            primaryKey.getBytes(StandardCharsets.UTF_8)));
                twoPhaseCommitter.prewriteSecondaryKeys(
                    primaryKey.getBytes(StandardCharsets.UTF_8), pairs.iterator(), 20000);
                BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(20000);

                long commitTs = session.getTimestamp().getVersion();

                twoPhaseCommitter.commitPrimaryKey(
                    backOffer, primaryKey.getBytes(StandardCharsets.UTF_8), commitTs);
              } catch (Exception e) {
                logger.error(e.getMessage(), e);
                failCount.incrementAndGet();
              } finally {
                latch.countDown();
              }
            })
        .start();

    Thread.sleep(10);
    new Thread(
            () -> {
              long startTS = session.getTimestamp().getVersion();
              try {
                TwoPhaseCommitter twoPhaseCommitter =
                    new TwoPhaseCommitter(
                        session, startTS, DEFAULT_BATCH_WRITE_LOCK_TTL, executorService);
                List<BytePairWrapper> pairs =
                    Arrays.asList(
                        new BytePairWrapper(
                            primaryKey.getBytes(StandardCharsets.UTF_8),
                            primaryKey.getBytes(StandardCharsets.UTF_8)));
                twoPhaseCommitter.prewriteSecondaryKeys(
                    primaryKey.getBytes(StandardCharsets.UTF_8), pairs.iterator(), 20000);
                BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(20000);

                long commitTs = session.getTimestamp().getVersion();

                twoPhaseCommitter.commitPrimaryKey(
                    backOffer, primaryKey.getBytes(StandardCharsets.UTF_8), commitTs);
              } catch (Exception e) {
                logger.error(e.getMessage(), e);
                failCount.incrementAndGet();
              } finally {
                latch.countDown();
              }
            })
        .start();
    latch.await();
    Assert.assertEquals(1, failCount.get());
  }
}
