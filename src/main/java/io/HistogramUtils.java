package io;

import io.prometheus.client.Histogram;
import java.lang.reflect.Field;

public class HistogramUtils {
  private static final double[] durationBuckets =
      new double[] {
        0.001D, 0.002D, 0.003D, 0.004D, 0.005D,
        0.008D, 0.010D, 0.012D, 0.015D, 0.020D,
        0.025D, 0.030D, 0.035D, 0.040D, 0.045D,
        0.050D, 0.060D, 0.07D, 0.080D, 0.090D,
        0.10D, 0.120D, 0.150D, 0.170D, 0.200D,
        0.4D, 0.5D, 0.6D, 0.7D, 0.8D,
        1D, 2.5D, 5D, 7.5D, 10D,
      };

  private static final double[] bytesBuckets;
  private static final Field timerStartField;

  static {
    bytesBuckets = new double[30];
    for (int i = 0; i < 30; ++i) {
      bytesBuckets[i] = 1 * Math.pow(1.7, (double) i);
    }

    try {
      timerStartField = Histogram.Timer.class.getField("start");
      timerStartField.setAccessible(true);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Histogram.Builder buildDuration() {
    return Histogram.build().buckets(durationBuckets);
  }

  public static Histogram.Builder buildBytes() {
    return Histogram.build().buckets(bytesBuckets);
  }

  public static long getHistogramTimerStart(Histogram.Timer timer) {
    try {
      return (long) timerStartField.get(timer);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
