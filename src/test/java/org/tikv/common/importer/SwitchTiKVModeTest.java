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

package org.tikv.common.importer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseRawKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class SwitchTiKVModeTest extends BaseRawKVTest {
  private TiSession session;

  @Before
  public void setup() {
    TiConfiguration conf = createTiConfiguration();
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void switchTiKVModeTest() throws InterruptedException {
    SwitchTiKVModeClient switchTiKVModeClient = session.getSwitchTiKVModeClient();
    for (int i = 0; i < 2; i++) {
      switchTiKVModeClient.keepTiKVToImportMode();
      Thread.sleep(6000);
      switchTiKVModeClient.stopKeepTiKVToImportMode();
      switchTiKVModeClient.switchTiKVToNormalMode();
    }
  }
}
