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

package org.tikv.service.failsafe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CircuitBreakerTest {

  @Test
  public void testCircuitBreaker() throws InterruptedException {
    boolean enable = true;
    int windowInSeconds = 2;
    int errorThresholdPercentage = 100;
    int requestVolumeThreshold = 10;
    int sleepWindowInSeconds = 1;
    int attemptRequestCount = 10;

    int sleepDelta = 100;

    CircuitBreakerImpl circuitBreaker =
        new CircuitBreakerImpl(
            enable,
            windowInSeconds,
            errorThresholdPercentage,
            requestVolumeThreshold,
            sleepWindowInSeconds,
            attemptRequestCount,
            1024);
    CircuitBreakerMetrics metrics = circuitBreaker.getMetrics();

    // initial state: CLOSE
    assertTrue(!circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.CLOSED);

    // CLOSE => OPEN
    for (int i = 1; i <= requestVolumeThreshold; i++) {
      metrics.recordFailure();
    }
    Thread.sleep(windowInSeconds * 1000 + sleepDelta);
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.OPEN);

    // OPEN => HALF_OPEN
    Thread.sleep(sleepWindowInSeconds * 1000);
    assertTrue(circuitBreaker.attemptExecution());
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.HALF_OPEN);

    // HALF_OPEN => OPEN
    circuitBreaker.recordAttemptFailure();
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.OPEN);

    // OPEN => HALF_OPEN
    Thread.sleep(sleepWindowInSeconds * 1000 + sleepDelta);
    assertTrue(circuitBreaker.attemptExecution());
    circuitBreaker.recordAttemptSuccess();
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.HALF_OPEN);

    // HALF_OPEN => CLOSED
    for (int i = 1; i < attemptRequestCount; i++) {
      assertTrue(circuitBreaker.attemptExecution());
      circuitBreaker.recordAttemptSuccess();
    }
    assertTrue(!circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.CLOSED);
  }
}
