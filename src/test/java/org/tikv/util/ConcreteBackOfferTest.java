package org.tikv.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffFunction.BackOffFuncType;
import org.tikv.common.util.ConcreteBackOffer;

public class ConcreteBackOfferTest {

  private static void ignoreException(Callable<Void> callable) throws Exception {
    try {
      callable.call();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void raceMapTest() throws Exception {
    ConcreteBackOffer backOffer = ConcreteBackOffer.newRawKVBackOff();
    ignoreException(
        () -> {
          backOffer.doBackOff(BackOffFuncType.BoRegionMiss, new Exception("first backoff"));
          return null;
        });
    ignoreException(
        () -> {
          backOffer.doBackOff(BackOffFuncType.BoTiKVRPC, new Exception("second backoff"));
          return null;
        });
    for (Entry<BackOffFuncType, BackOffFunction> item : backOffer.backOffFunctionMap.entrySet()) {
      backOffer.backOffFunctionMap.remove(item.getKey());
    }
  }

  @Test
  public void raceErrorsTest() throws Exception {
    int timeToSleep = 1;
    int threadCnt = Runtime.getRuntime().availableProcessors() * 2;
    int taskCnt = threadCnt * 2;

    ExecutorService executorService = Executors.newFixedThreadPool(threadCnt);
    ConcreteBackOffer backOffer = ConcreteBackOffer.newCustomBackOff(timeToSleep);
    List<Future<?>> tasks = new ArrayList<>();
    for (int i = 0; i < taskCnt; i++) {
      int idx = i;
      Future<?> task =
          executorService.submit(
              () -> {
                try {
                  backOffer.doBackOff(
                      BackOffFuncType.BoUpdateLeader, new Exception("backoff " + idx));
                } catch (GrpcException ignored) {
                }
              });
      tasks.add(task);
    }
    for (Future<?> task : tasks) {
      task.get();
    }
    Assert.assertEquals(backOffer.errors.size(), taskCnt);
  }
}
