package org.tikv.common.failsafe;

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

    CircuitBreakerImpl circuitBreaker =
        new CircuitBreakerImpl(
            enable,
            windowInSeconds,
            errorThresholdPercentage,
            requestVolumeThreshold,
            sleepWindowInSeconds,
            attemptRequestCount);
    CircuitBreakerMetrics metrics = circuitBreaker.getMetrics();

    // initial state: CLOSE
    assertTrue(!circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.CLOSED);

    // CLOSE => OPEN
    for (int i = 1; i <= requestVolumeThreshold; i++) {
      metrics.failure();
    }
    Thread.sleep(windowInSeconds * 1000);
    metrics.failure();
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.OPEN);

    // OPEN => HALF_OPEN
    Thread.sleep(sleepWindowInSeconds * 1000);
    assertTrue(circuitBreaker.attemptExecution());
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.HALF_OPEN);

    // HALF_OPEN => OPEN
    circuitBreaker.markAttemptFailure();
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.OPEN);

    // OPEN => HALF_OPEN
    Thread.sleep(sleepWindowInSeconds * 1000);
    assertTrue(circuitBreaker.attemptExecution());
    circuitBreaker.markAttemptSuccess();
    assertTrue(circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.HALF_OPEN);

    // HALF_OPEN => CLOSED
    for (int i = 1; i < attemptRequestCount; i++) {
      assertTrue(circuitBreaker.attemptExecution());
      circuitBreaker.markAttemptSuccess();
    }
    assertTrue(!circuitBreaker.isOpen());
    assertEquals(circuitBreaker.getStatus(), CircuitBreaker.Status.CLOSED);
  }
}
