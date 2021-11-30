package org.tikv.common.util;

import java.util.concurrent.ThreadLocalRandom;

public class BackOffFunction {
  private final int base;
  private final int cap;
  private final BackOffer.BackOffStrategy strategy;
  private long lastSleep;
  private int attempts;

  private BackOffFunction(int base, int cap, BackOffer.BackOffStrategy strategy) {
    this.base = base;
    this.cap = cap;
    this.strategy = strategy;
    lastSleep = base;
  }

  public static BackOffFunction create(int base, int cap, BackOffer.BackOffStrategy strategy) {
    return new BackOffFunction(base, cap, strategy);
  }

  /**
   * Do back off in exponential with optional jitters according to different back off strategies.
   * See http://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  long getSleepMs(long maxSleepMs) {
    long sleep = 0;
    long v = expo(base, cap, attempts);
    switch (strategy) {
      case NoJitter:
        sleep = v;
        break;
      case FullJitter:
        sleep = ThreadLocalRandom.current().nextLong(v);
        break;
      case EqualJitter:
        sleep = v / 2 + ThreadLocalRandom.current().nextLong(v / 2);
        break;
      case DecorrJitter:
        sleep = Math.min(cap, base + ThreadLocalRandom.current().nextLong(lastSleep * 3 - base));
        break;
    }

    if (maxSleepMs > 0 && sleep > maxSleepMs) {
      sleep = maxSleepMs;
    }

    attempts++;
    lastSleep = sleep;
    return lastSleep;
  }

  private int expo(int base, int cap, int n) {
    return (int) Math.min(cap, base * Math.pow(2.0d, n));
  }

  public enum BackOffFuncType {
    BoTiKVRPC,
    BoTxnLock,
    BoTxnLockFast,
    BoPDRPC,
    BoRegionMiss,
    BoUpdateLeader,
    BoServerBusy,
    BoTxnNotFound,
    CheckTimeout
  }
}
