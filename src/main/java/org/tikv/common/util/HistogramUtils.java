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

import io.prometheus.client.Histogram;

public class HistogramUtils {
  private static final double[] DURATION_BUCKETS =
      new double[] {
        0.001D, 0.002D, 0.003D, 0.004D, 0.005D,
        0.008D, 0.010D, 0.012D, 0.015D, 0.020D,
        0.025D, 0.030D, 0.035D, 0.040D, 0.045D,
        0.050D, 0.060D, 0.07D, 0.080D, 0.090D,
        0.10D, 0.120D, 0.150D, 0.170D, 0.200D,
        0.4D, 0.5D, 0.6D, 0.7D, 0.8D,
        1D, 2.5D, 5D, 7.5D, 10D,
      };

  private static final double[] BYTES_BUCKETS;

  static {
    BYTES_BUCKETS = new double[30];
    for (int i = 0; i < 30; ++i) {
      BYTES_BUCKETS[i] = 1 * Math.pow(1.7, i);
    }
  }

  public static Histogram.Builder buildDuration() {
    return Histogram.build().buckets(DURATION_BUCKETS);
  }

  public static Histogram.Builder buildBytes() {
    return Histogram.build().buckets(BYTES_BUCKETS);
  }
}
