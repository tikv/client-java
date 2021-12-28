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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class CircuitBreakerMetricsTest {
  private static final int TEST_COUNT = 10;
  private static final int WINDOW_IN_SECONDS = 1;
  private static final int SLEEP_DELTA = 100;

  @Test
  public void testAllSuccess() throws InterruptedException, IOException {
    CircuitBreakerMetricsImpl metrics = new CircuitBreakerMetricsImpl(WINDOW_IN_SECONDS);

    AtomicReference<HealthCounts> healthCounts = new AtomicReference<>();
    MetricsListener metricsListener = healthCounts::set;
    metrics.addListener(metricsListener);

    for (int i = 1; i <= TEST_COUNT; i++) {
      metrics.recordSuccess();
    }
    Thread.sleep(WINDOW_IN_SECONDS * 1000 + SLEEP_DELTA);
    assertNotNull(healthCounts.get());
    assertEquals(healthCounts.get().getTotalRequests(), TEST_COUNT);
    assertEquals(healthCounts.get().getErrorPercentage(), 0);
    metrics.close();
  }

  @Test
  public void testAllFailure() throws InterruptedException, IOException {
    CircuitBreakerMetricsImpl metrics = new CircuitBreakerMetricsImpl(WINDOW_IN_SECONDS);

    AtomicReference<HealthCounts> healthCounts = new AtomicReference<>();
    MetricsListener metricsListener = healthCounts::set;
    metrics.addListener(metricsListener);

    for (int i = 1; i <= TEST_COUNT; i++) {
      metrics.recordFailure();
    }
    Thread.sleep(WINDOW_IN_SECONDS * 1000 + SLEEP_DELTA);
    assertNotNull(healthCounts.get());
    assertEquals(healthCounts.get().getTotalRequests(), TEST_COUNT);
    assertEquals(healthCounts.get().getErrorPercentage(), 100);
    metrics.close();
  }

  @Test
  public void testHalfFailure() throws InterruptedException, IOException {
    CircuitBreakerMetricsImpl metrics = new CircuitBreakerMetricsImpl(WINDOW_IN_SECONDS);

    AtomicReference<HealthCounts> healthCounts = new AtomicReference<>();
    MetricsListener metricsListener = healthCounts::set;
    metrics.addListener(metricsListener);

    for (int i = 1; i <= TEST_COUNT; i++) {
      metrics.recordFailure();
      metrics.recordSuccess();
    }
    Thread.sleep(WINDOW_IN_SECONDS * 1000 + SLEEP_DELTA);
    assertNotNull(healthCounts.get());
    assertEquals(healthCounts.get().getTotalRequests(), TEST_COUNT * 2);
    assertEquals(healthCounts.get().getErrorPercentage(), 50);
    metrics.close();
  }
}
