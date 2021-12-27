/*
 * Copyright 2021 TiKV Project Authors.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseRawKVTest;

public class PDClientIntegrationTest extends BaseRawKVTest {
  private TiSession session;

  @Before
  public void setup() {
    TiConfiguration conf = createTiConfiguration();
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
        Thread.sleep(2000);
        assertTrue(client.isCheckerPaused(checker));

        client.stopKeepPauseChecker(checker);
        Thread.sleep(2000);

        client.resumeChecker(checker);
        assertFalse(client.isCheckerPaused(checker));
      }
    }
  }
}
