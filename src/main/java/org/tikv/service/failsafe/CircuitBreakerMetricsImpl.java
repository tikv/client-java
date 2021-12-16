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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CircuitBreakerMetricsImpl implements CircuitBreakerMetrics {

  private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerMetricsImpl.class);

  private final int windowInMS;
  private final List<MetricsListener> listeners;
  private final AtomicReference<SingleWindowMetrics> currentMetrics;

  private final ScheduledExecutorService scheduler;
  private static final int SCHEDULER_INITIAL_DELAY = 1000;
  private static final int SCHEDULER_PERIOD = 1000;

  public CircuitBreakerMetricsImpl(int windowInSeconds) {
    this.windowInMS = windowInSeconds * 1000;
    this.listeners = new ArrayList<>();
    this.currentMetrics = new AtomicReference<>(new SingleWindowMetrics());

    scheduler =
        new ScheduledThreadPoolExecutor(
            1,
            new BasicThreadFactory.Builder()
                .namingPattern("circuit-breaker-metrics-%d")
                .daemon(true)
                .build());

    scheduler.scheduleAtFixedRate(
        this::onReachCircuitWindow,
        SCHEDULER_INITIAL_DELAY,
        SCHEDULER_PERIOD,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void recordSuccess() {
    currentMetrics.get().recordSuccess();
  }

  @Override
  public void recordFailure() {
    currentMetrics.get().recordFailure();
  }

  private void onReachCircuitWindow() {
    SingleWindowMetrics singleWindowMetrics = currentMetrics.get();
    if (System.currentTimeMillis() < singleWindowMetrics.getStartMS() + windowInMS) {
      return;
    }
    if (!currentMetrics.compareAndSet(singleWindowMetrics, new SingleWindowMetrics())) {
      return;
    }
    logger.atDebug().log("window timeout, reset SingleWindowMetrics");
    HealthCounts healthCounts = singleWindowMetrics.getHealthCounts();
    for (MetricsListener metricsListener : listeners) {
      metricsListener.onNext(healthCounts);
    }
  }

  @Override
  public void addListener(MetricsListener metricsListener) {
    listeners.add(metricsListener);
  }

  @Override
  public void close() throws IOException {
    scheduler.shutdown();
  }

  /** Instead of using SingleWindowMetrics, it is better to use RollingWindowMetrics. */
  static class SingleWindowMetrics {

    private final long startMS = System.currentTimeMillis();
    private final AtomicLong totalCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public void recordSuccess() {
      totalCount.incrementAndGet();
    }

    public void recordFailure() {
      totalCount.incrementAndGet();

      errorCount.incrementAndGet();
    }

    public HealthCounts getHealthCounts() {
      return new HealthCounts(totalCount.get(), errorCount.get());
    }

    public long getStartMS() {
      return startMS;
    }
  }
}
