/*
 * Copyright 2019 The TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
      TiConfiguration conf = TiConfiguration.createDefault(DEFAULT_PD_ADDRESS);
      // might be overwritten by saved values in pd.
      conf.setGCRunInterval(6 * 1000);
      conf.setGCLifeTime(2 * 1000);
      conf.setGCWaitTime(3 * 1000);
      conf.setGCWorkerLease(2 * 1000);
      TiSession session = TiSession.create(conf);
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

  // Test GC without doing anything
  @Test
  public void testGC() {
    if (!initialized) return;
    try {
      Thread.sleep(60 * 1000);
    } catch (InterruptedException e) {
      // ignore
    }
  }
}
