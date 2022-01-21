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

package org.tikv.common.log;

import org.junit.Assert;
import org.junit.Test;

public class SlowLogImplTest {

  @Test
  public void testThresholdTime() throws InterruptedException {
    SlowLogImpl slowLog = new SlowLogImpl(1000);
    Thread.sleep(1000);
    Assert.assertTrue(slowLog.timeExceeded());

    slowLog = new SlowLogImpl(1000);
    Thread.sleep(500);
    Assert.assertFalse(slowLog.timeExceeded());
  }
}
