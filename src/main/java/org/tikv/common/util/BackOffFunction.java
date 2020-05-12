package org.tikv.common.util;

import java.util.concurrent.ThreadLocalRandom;
import org.tikv.common.exception.GrpcException;

public class BackOffFunction {
  private final int base;
  private final int cap;
  private final BackOffer.BackOffStrategy strategy;

  private int lastSleep;
  private int attempts;

  public static BackOffFunction create(int base, int cap, BackOffer.BackOffStrategy strategy) {
    return new BackOffFunction(base, cap, strategy);
  }

  private BackOffFunction(int base, int cap, BackOffer.BackOffStrategy strategy) {
    this.base = base;
    this.cap = cap;
    this.strategy = strategy;
    lastSleep = base;
  }

  public enum BackOffFuncType {
    BoTiKVRPC,
    BoTxnLock,
    BoTxnLockFast,
    BoPDRPC,
    BoRegionMiss,
    BoUpdateLeader,
    BoServerBusy,
    BoStoreNotMatch
  }

  /**
   * Do back off in exponential with optional jitters according to different back off strategies.
   * See http://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  public int doBackOff() {
    int sleep = 0;
    int v = expo(base, cap, attempts);
    switch (strategy) {
      case NoJitter:
        sleep = v;
        break;
      case FullJitter:
        sleep = ThreadLocalRandom.current().nextInt(v);
        break;
      case EqualJitter:
        sleep = v / 2 + ThreadLocalRandom.current().nextInt(v / 2);
        break;
      case DecorrJitter:
        sleep = Math.min(cap, base + ThreadLocalRandom.current().nextInt(lastSleep * 3 - base));
        break;
    }

    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      throw new GrpcException(e);
    }
    attempts++;
    lastSleep = sleep;
    return lastSleep;
  }

  private int expo(int base, int cap, int n) {
    return (int) Math.min(cap, base * Math.pow(2.0d, n));
  }
}
