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

import java.io.Closeable;

public interface CircuitBreaker extends Closeable {

  enum Status {
    CLOSED(0),
    HALF_OPEN(1),
    OPEN(2);

    private final int value;

    private Status(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * Every requests asks this if it is allowed to proceed or not. It is idempotent and does not
   * modify any internal state.
   *
   * @return boolean whether a request should be permitted
   */
  boolean allowRequest();

  /**
   * Invoked at start of command execution to attempt an execution. This is non-idempotent - it may
   * modify internal state.
   */
  boolean attemptExecution();

  /** Invoked on successful executions as part of feedback mechanism when in a half-open state. */
  void recordAttemptSuccess();

  /** Invoked on unsuccessful executions as part of feedback mechanism when in a half-open state. */
  void recordAttemptFailure();

  /** Get the Circuit Breaker Metrics Object. */
  CircuitBreakerMetrics getMetrics();
}
