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

package org.tikv.common.util;

import org.tikv.common.log.SlowLog;

public interface BackOffer {
  // Back off types.
  int seconds = 1000;
  int TSO_MAX_BACKOFF = 5 * seconds;
  int SCANNER_NEXT_MAX_BACKOFF = 40 * seconds;
  int BATCH_GET_MAX_BACKOFF = 40 * seconds;
  int COP_NEXT_MAX_BACKOFF = 40 * seconds;
  int GET_MAX_BACKOFF = 40 * seconds;
  int RAWKV_MAX_BACKOFF = 20 * seconds;
  int PD_INFO_BACKOFF = 5 * seconds;
  int TIKV_SWITCH_MODE_BACKOFF = seconds;
  int SPLIT_REGION_BACKOFF = 12000;
  int SCATTER_REGION_BACKOFF = 30000;
  int INGEST_BACKOFF = 30000;

  /**
   * doBackOff sleeps a while base on the BackOffType and records the error message. Will stop until
   * max back off time exceeded and throw an exception to the caller.
   */
  void doBackOff(BackOffFunction.BackOffFuncType funcType, Exception err);

  /** check if deadline exceeded. */
  void checkTimeout();

  /**
   * canRetryAfterSleep sleeps a while base on the BackOffType and records the error message. Will
   * stop until max back off time exceeded and throw an exception to the caller. It will return
   * false if the total sleep time has exceed some limit condition.
   */
  boolean canRetryAfterSleep(BackOffFunction.BackOffFuncType funcType);

  /**
   * BackoffWithMaxSleep sleeps a while base on the backoffType and records the error message and
   * never sleep more than maxSleepMs for each sleep.
   */
  void doBackOffWithMaxSleep(
      BackOffFunction.BackOffFuncType funcType, long maxSleepMs, Exception err);

  // Back off strategies
  enum BackOffStrategy {
    // NoJitter makes the backoff sequence strict exponential.
    NoJitter,
    // FullJitter applies random factors to strict exponential.
    FullJitter,
    // EqualJitter is also randomized, but prevents very short sleeps.
    EqualJitter,
    // DecorrJitter increases the maximum jitter based on the last random value.
    DecorrJitter
  }

  SlowLog getSlowLog();

  Long getClusterId();
}
