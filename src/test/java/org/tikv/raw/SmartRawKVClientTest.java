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

package org.tikv.raw;

import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseRawKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.CircuitBreakerOpenException;

public class SmartRawKVClientTest extends BaseRawKVTest {
  private boolean enable = true;
  private int windowInSeconds = 2;
  private int errorThresholdPercentage = 100;
  private int requestVolumeThreshold = 10;
  private int sleepWindowInSeconds = 1;
  private int attemptRequestCount = 10;

  private int sleepDelta = 100;

  private TiSession session;
  private SmartRawKVClient client;

  @Before
  public void setup() {
    TiConfiguration conf = createTiConfiguration();
    conf.setCircuitBreakEnable(enable);
    conf.setEnableAtomicForCAS(enable);
    conf.setCircuitBreakAvailabilityWindowInSeconds(windowInSeconds);
    conf.setCircuitBreakAvailabilityErrorThresholdPercentage(errorThresholdPercentage);
    conf.setCircuitBreakAvailabilityRequestVolumnThreshold(requestVolumeThreshold);
    conf.setCircuitBreakSleepWindowInSeconds(sleepWindowInSeconds);
    conf.setCircuitBreakAttemptRequestCount(attemptRequestCount);
    session = TiSession.create(conf);
    client = session.createSmartRawClient();
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void testCircuitBreaker() throws InterruptedException {
    // CLOSED => OPEN
    {
      for (int i = 1; i <= requestVolumeThreshold; i++) {
        error();
      }
      Thread.sleep(windowInSeconds * 1000 + sleepDelta);

      Exception error = null;
      try {
        client.get(ByteString.copyFromUtf8("key"));
        assertTrue(false);
      } catch (Exception e) {
        error = e;
      }
      assertTrue(error instanceof CircuitBreakerOpenException);
    }

    // OPEN => CLOSED
    {
      Thread.sleep(sleepWindowInSeconds * 1000);
      for (int i = 1; i <= attemptRequestCount; i++) {
        success();
      }
      client.get(ByteString.copyFromUtf8("key"));
    }
  }

  @Test
  public void testMultiClients() throws InterruptedException {
    for (int i = 0; i < 10240; i++) {
      client = session.createSmartRawClient();
    }
  }

  private void success() {
    client.get(ByteString.copyFromUtf8("key"));
  }

  private void error() {
    try {
      client.callWithCircuitBreaker("error", () -> 1 / 0);
    } catch (Exception ignored) {
    }
  }
}
