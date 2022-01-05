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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
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
}
