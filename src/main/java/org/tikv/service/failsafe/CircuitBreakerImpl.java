/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.service.failsafe;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;

public class CircuitBreakerImpl implements CircuitBreaker {
  private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerImpl.class);

  private final boolean enable;
  private final int windowInSeconds;
  private final int errorThresholdPercentage;
  private final int requestVolumeThreshold;
  private final int sleepWindowInSeconds;
  private final int attemptRequestCount;

  private final AtomicLong circuitOpened = new AtomicLong(-1);
  private final AtomicReference<Status> status = new AtomicReference<>(Status.CLOSED);
  private final AtomicLong attemptCount = new AtomicLong(0);
  private final AtomicLong attemptSuccessCount = new AtomicLong(0);

  private final CircuitBreakerMetrics metrics;

  public CircuitBreakerImpl(TiConfiguration conf) {
    this(
        conf.isCircuitBreakEnable(),
        conf.getCircuitBreakAvailabilityWindowInSeconds(),
        conf.getCircuitBreakAvailabilityErrorThresholdPercentage(),
        conf.getCircuitBreakAvailabilityRequestVolumnThreshold(),
        conf.getCircuitBreakSleepWindowInSeconds(),
        conf.getCircuitBreakAttemptRequestCount());
  }

  public CircuitBreakerImpl(
      boolean enable,
      int windowInSeconds,
      int errorThresholdPercentage,
      int requestVolumeThreshold,
      int sleepWindowInSeconds,
      int attemptRequestCount) {
    this.enable = enable;
    this.windowInSeconds = windowInSeconds;
    this.errorThresholdPercentage = errorThresholdPercentage;
    this.requestVolumeThreshold = requestVolumeThreshold;
    this.sleepWindowInSeconds = sleepWindowInSeconds;
    this.attemptRequestCount = attemptRequestCount;
    this.metrics = new CircuitBreakerMetricsImpl(windowInSeconds);
    this.metrics.addListener(getMetricsListener());
  }

  private MetricsListener getMetricsListener() {
    return hc -> {
      logger.info("onNext " + hc.toString());
      // check if we are past the requestVolumeThreshold
      if (hc.getTotalRequests() < requestVolumeThreshold) {
        // we are not past the minimum volume threshold for the stat window,
        // so no change to circuit status.
        // if it was CLOSED, it stays CLOSED
        // if it was half-open, we need to wait for some successful command executions
        // if it was open, we need to wait for sleep window to elapse
      } else {
        if (hc.getErrorPercentage() < errorThresholdPercentage) {
          // we are not past the minimum error threshold for the stat window,
          // so no change to circuit status.
          // if it was CLOSED, it stays CLOSED
          // if it was half-open, we need to wait for some successful command executions
          // if it was open, we need to wait for sleep window to elapse
        } else {
          // our failure rate is too high, we need to set the state to OPEN
          close2Open();
        }
      }
    };
  }

  @Override
  public CircuitBreakerMetrics getMetrics() {
    return metrics;
  }

  @Override
  public boolean allowRequest() {
    if (!enable) {
      return true;
    }
    return !isOpen();
  }

  boolean isOpen() {
    return circuitOpened.get() >= 0;
  }

  Status getStatus() {
    return status.get();
  }

  @Override
  public void recordAttemptSuccess() {
    if (attemptSuccessCount.incrementAndGet() >= this.attemptRequestCount) {
      halfOpen2Close();
    }
  }

  @Override
  public void recordAttemptFailure() {
    halfOpen2Open();
  }

  @Override
  public boolean attemptExecution() {
    if (allowRequest()) {
      return true;
    } else {
      if (isAfterSleepWindow()) {
        // only the `attemptRequestCount` requests after sleep window should execute
        // if all the executing commands succeed, the status will transition to CLOSED
        // if some of the executing commands fail, the status will transition to OPEN
        open2HalfOpen();
        return attemptCount.incrementAndGet() <= attemptRequestCount;
      } else {
        return false;
      }
    }
  }

  private boolean isAfterSleepWindow() {
    final long circuitOpenTime = circuitOpened.get();
    final long currentTime = System.currentTimeMillis();
    final long sleepWindowTime = (long) sleepWindowInSeconds * 1000;
    return currentTime >= circuitOpenTime + sleepWindowTime;
  }

  private void close2Open() {
    if (status.compareAndSet(Status.CLOSED, Status.OPEN)) {
      // This thread wins the race to open the circuit
      // it sets the start time for the sleep window
      circuitOpened.set(System.currentTimeMillis());
      logger.info("CLOSED => OPEN");
    }
  }

  private void halfOpen2Close() {
    if (status.compareAndSet(Status.HALF_OPEN, Status.CLOSED)) {
      // This thread wins the race to close the circuit
      circuitOpened.set(-1L);
      logger.info("HALF_OPEN => CLOSED");
    }
  }

  private void open2HalfOpen() {
    if (status.compareAndSet(Status.OPEN, Status.HALF_OPEN)) {
      // This thread wins the race to half close the circuit
      // it resets the attempt count
      attemptCount.set(0);
      attemptSuccessCount.set(0);
      logger.info("OPEN => HALF_OPEN");
    }
  }

  private void halfOpen2Open() {
    if (status.compareAndSet(Status.HALF_OPEN, Status.OPEN)) {
      // This thread wins the race to re-open the circuit
      // it resets the start time for the sleep window
      circuitOpened.set(System.currentTimeMillis());
      logger.info("HALF_OPEN => OPEN");
    }
  }

  @Override
  public void close() throws IOException {
    metrics.close();
  }
}
