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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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

  @Test
  public void testSlowLogJson() throws InterruptedException {
    SlowLogImpl slowLog = new SlowLogImpl(1);
    SlowLogSpan span = slowLog.start("method1");
    Thread.sleep(500);
    span.end();
    JsonObject object = slowLog.getSlowLogJson();
    System.out.println(object);

    JsonArray spans = object.get("spans").getAsJsonArray();
    Assert.assertEquals(1, spans.size());
    JsonObject spanObject = spans.get(0).getAsJsonObject();
    Assert.assertEquals("method1", spanObject.get("event").getAsString());
    Assert.assertTrue(spanObject.get("duration_ms").getAsLong() > 500);
  }

  @Test
  public void testUnsignedLong() {
    Assert.assertEquals("12345", SlowLogImpl.toUnsignedBigInteger(12345L).toString());
    Assert.assertEquals("18446744073709551615", SlowLogImpl.toUnsignedBigInteger(-1L).toString());
    Assert.assertEquals("18446744073709551614", SlowLogImpl.toUnsignedBigInteger(-2L).toString());
  }
}
