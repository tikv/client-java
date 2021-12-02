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

package org.tikv.common.failsafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CircuitBreakerMetricsImpl implements CircuitBreakerMetrics {
  private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerMetricsImpl.class);

  private final int windowInMS;
  private final List<MetricsListener> listeners;
  private final AtomicReference<SingleWindowMetrics> currentMetrics;

  public CircuitBreakerMetricsImpl(int windowInSeconds) {
    this.windowInMS = windowInSeconds * 1000;
    listeners = new ArrayList<>();
    this.currentMetrics = new AtomicReference<>(new SingleWindowMetrics());
  }

  @Override
  public void success() {
    currentMetrics.get().success();
    checkTimeout();
  }

  @Override
  public void failure() {
    currentMetrics.get().failure();
    checkTimeout();
  }

  private void checkTimeout() {
    SingleWindowMetrics singleWindowMetrics = currentMetrics.get();
    if (System.currentTimeMillis() >= singleWindowMetrics.getStartMS() + windowInMS) {
      if (currentMetrics.compareAndSet(singleWindowMetrics, new SingleWindowMetrics())) {
        logger.info("window timeout, reset SingleWindowMetrics");
        HealthCounts healthCounts = singleWindowMetrics.getHealthCounts();
        for (MetricsListener metricsListener : listeners) {
          metricsListener.onNext(healthCounts);
        }
      }
    }
  }

  @Override
  public void addListener(MetricsListener metricsListener) {
    listeners.add(metricsListener);
  }

  /** Instead of using SingleWindowMetrics, it is better to use RollingWindowMetrics. */
  static class SingleWindowMetrics {
    private final long startMS = System.currentTimeMillis();
    private final AtomicLong totalCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public void success() {
      totalCount.incrementAndGet();
    }

    public void failure() {
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
