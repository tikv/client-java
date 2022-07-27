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

package org.tikv.common;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tikv.raw.RawKVClient;

public class TimeoutTest extends MockThreeStoresTest {
  @Before
  public void init() throws Exception {
    updateConf(
        conf -> {
          conf.setEnableAtomicForCAS(true);
          conf.setTimeout(150);
          conf.setForwardTimeout(200);
          conf.setRawKVReadTimeoutInMS(400);
          conf.setRawKVWriteTimeoutInMS(400);
          conf.setRawKVBatchReadTimeoutInMS(400);
          conf.setRawKVBatchWriteTimeoutInMS(400);
          conf.setRawKVWriteSlowLogInMS(50);
          conf.setRawKVReadSlowLogInMS(50);
          conf.setRawKVBatchReadSlowLogInMS(50);
          conf.setRawKVBatchWriteSlowLogInMS(50);
          return conf;
        });
  }

  private RawKVClient createClient() {
    return session.createRawClient();
  }

  @Test
  public void testTimeoutInTime() {
    try (RawKVClient client = createClient()) {
      pdServers.get(0).stop();
      long start = System.currentTimeMillis();
      client.get(ByteString.copyFromUtf8("key"));
      long end = System.currentTimeMillis();
      Assert.assertTrue(end - start < 500);
    }
  }
}
