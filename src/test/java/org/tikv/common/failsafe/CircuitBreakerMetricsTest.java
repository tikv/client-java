package org.tikv.common.failsafe;

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
      metrics.success();
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
      metrics.failure();
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
      metrics.failure();
      metrics.success();
    }
    Thread.sleep(WINDOW_IN_SECONDS * 1000 + SLEEP_DELTA);
    assertNotNull(healthCounts.get());
    assertEquals(healthCounts.get().getTotalRequests(), TEST_COUNT * 2);
    assertEquals(healthCounts.get().getErrorPercentage(), 50);
    metrics.close();
  }
}
