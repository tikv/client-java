/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package org.tikv.common.util;

import static org.tikv.common.ConfigUtils.TIKV_BO_REGION_MISS_BASE_IN_MS;

import com.google.common.base.Preconditions;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;

public class ConcreteBackOffer implements BackOffer {
  private static final Logger logger = LoggerFactory.getLogger(ConcreteBackOffer.class);
  private final int maxSleep;
  private final Map<BackOffFunction.BackOffFuncType, BackOffFunction> backOffFunctionMap;
  private final List<Exception> errors;
  private int totalSleep;
  private final long deadline;

  public static final Histogram BACKOFF_DURATION =
      Histogram.build()
          .name("client_java_backoff_duration")
          .help("backoff duration.")
          .labelNames("type")
          .register();

  private ConcreteBackOffer(int maxSleep, long deadline) {
    Preconditions.checkArgument(
        maxSleep == 0 || deadline == 0, "Max sleep time should be 0 or Deadline should be 0.");
    Preconditions.checkArgument(maxSleep >= 0, "Max sleep time cannot be less than 0.");
    Preconditions.checkArgument(deadline >= 0, "Deadline cannot be less than 0.");
    this.maxSleep = maxSleep;
    this.errors = new ArrayList<>();
    this.backOffFunctionMap = new HashMap<>();
    this.deadline = deadline;
  }

  private ConcreteBackOffer(ConcreteBackOffer source) {
    this.maxSleep = source.maxSleep;
    this.totalSleep = source.totalSleep;
    this.errors = source.errors;
    this.backOffFunctionMap = source.backOffFunctionMap;
    this.deadline = source.deadline;
  }

  public static ConcreteBackOffer newDeadlineBackOff(int timeoutInMs) {
    long deadline = System.currentTimeMillis() + timeoutInMs;
    return new ConcreteBackOffer(0, deadline);
  }

  public static ConcreteBackOffer newCustomBackOff(int maxSleep) {
    return new ConcreteBackOffer(maxSleep, 0);
  }

  public static ConcreteBackOffer newScannerNextMaxBackOff() {
    return new ConcreteBackOffer(SCANNER_NEXT_MAX_BACKOFF, 0);
  }

  public static ConcreteBackOffer newBatchGetMaxBackOff() {
    return new ConcreteBackOffer(BATCH_GET_MAX_BACKOFF, 0);
  }

  public static ConcreteBackOffer newCopNextMaxBackOff() {
    return new ConcreteBackOffer(COP_NEXT_MAX_BACKOFF, 0);
  }

  public static ConcreteBackOffer newGetBackOff() {
    return new ConcreteBackOffer(GET_MAX_BACKOFF, 0);
  }

  public static ConcreteBackOffer newRawKVBackOff() {
    return new ConcreteBackOffer(RAWKV_MAX_BACKOFF, 0);
  }

  public static ConcreteBackOffer newTsoBackOff() {
    return new ConcreteBackOffer(TSO_MAX_BACKOFF, 0);
  }

  public static ConcreteBackOffer create(BackOffer source) {
    return new ConcreteBackOffer(((ConcreteBackOffer) source));
  }

  /**
   * Creates a back off func which implements exponential back off with optional jitters according
   * to different back off strategies. See http://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  private BackOffFunction createBackOffFunc(BackOffFunction.BackOffFuncType funcType) {
    BackOffFunction backOffFunction = null;
    switch (funcType) {
      case BoUpdateLeader:
        backOffFunction = BackOffFunction.create(1, 10, BackOffStrategy.NoJitter);
        break;
      case BoTxnLockFast:
        backOffFunction = BackOffFunction.create(100, 3000, BackOffStrategy.EqualJitter);
        break;
      case BoServerBusy:
        backOffFunction = BackOffFunction.create(2000, 10000, BackOffStrategy.EqualJitter);
        break;
      case BoRegionMiss:
        backOffFunction =
            BackOffFunction.create(
                TiConfiguration.getInt(TIKV_BO_REGION_MISS_BASE_IN_MS),
                500,
                BackOffStrategy.NoJitter);
        break;
      case BoTxnLock:
        backOffFunction = BackOffFunction.create(200, 3000, BackOffStrategy.EqualJitter);
        break;
      case BoPDRPC:
        backOffFunction = BackOffFunction.create(100, 600, BackOffStrategy.EqualJitter);
        break;
      case BoTiKVRPC:
        backOffFunction = BackOffFunction.create(100, 400, BackOffStrategy.EqualJitter);
        break;
      case BoTxnNotFound:
        backOffFunction = BackOffFunction.create(2, 500, BackOffStrategy.NoJitter);
        break;
    }
    return backOffFunction;
  }

  @Override
  public void doBackOff(BackOffFunction.BackOffFuncType funcType, Exception err) {
    doBackOffWithMaxSleep(funcType, -1, err);
  }

  @Override
  public boolean canRetryAfterSleep(BackOffFunction.BackOffFuncType funcType) {
    return canRetryAfterSleep(funcType, -1);
  }

  public boolean canRetryAfterSleep(BackOffFunction.BackOffFuncType funcType, long maxSleepMs) {
    Histogram.Timer backOffTimer = BACKOFF_DURATION.labels(funcType.name()).startTimer();
    BackOffFunction backOffFunction =
        backOffFunctionMap.computeIfAbsent(funcType, this::createBackOffFunc);

    // Back off will not be done here
    long sleep = backOffFunction.getSleepMs(maxSleepMs);
    totalSleep += sleep;
    // Check deadline
    if (deadline > 0) {
      long currentMs = System.currentTimeMillis();
      if (currentMs + sleep >= deadline) {
        logger.warn(String.format("Deadline %d is exceeded, errors:", deadline));
        return false;
      }
    }

    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      throw new GrpcException(e);
    }
    backOffTimer.observeDuration();
    if (maxSleep > 0 && totalSleep >= maxSleep) {
      logger.warn(String.format("BackOffer.maxSleep %dms is exceeded, errors:", maxSleep));
      return false;
    }
    return true;
  }

  @Override
  public void doBackOffWithMaxSleep(
      BackOffFunction.BackOffFuncType funcType, long maxSleepMs, Exception err) {
    logger.debug(
        String.format(
            "%s, retry later(totalSleep %dms, maxSleep %dms)",
            err.getMessage(), totalSleep, maxSleep));
    errors.add(err);
    if (!canRetryAfterSleep(funcType, maxSleepMs)) {
      logThrowError(err);
    }
  }

  private void logThrowError(Exception err) {
    StringBuilder errMsg = new StringBuilder();
    for (int i = 0; i < errors.size(); i++) {
      Exception curErr = errors.get(i);
      // Print only last 3 errors for non-DEBUG log levels.
      if (logger.isDebugEnabled() || i >= errors.size() - 3) {
        errMsg.append("\n").append(i).append(".").append(curErr.toString());
      }
    }
    logger.warn(errMsg.toString());
    // Use the last backoff type to generate an exception
    throw new GrpcException("retry is exhausted.", err);
  }
}
