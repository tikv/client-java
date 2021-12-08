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

public class HealthCounts {
  private final long totalCount;
  private final long errorCount;
  private final int errorPercentage;

  HealthCounts(long total, long error) {
    this.totalCount = total;
    this.errorCount = error;
    if (totalCount > 0) {
      this.errorPercentage = (int) ((double) errorCount / totalCount * 100);
    } else {
      this.errorPercentage = 0;
    }
  }

  public long getTotalRequests() {
    return totalCount;
  }

  public long getErrorCount() {
    return errorCount;
  }

  public int getErrorPercentage() {
    return errorPercentage;
  }

  @Override
  public String toString() {
    return "HealthCounts{"
        + "totalCount="
        + totalCount
        + ", errorCount="
        + errorCount
        + ", errorPercentage="
        + errorPercentage
        + '}';
  }
}
