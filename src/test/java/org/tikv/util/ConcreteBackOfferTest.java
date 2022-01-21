/*
<<<<<<< HEAD
 * Copyright 2017 PingCAP, Inc.
=======
 * Copyright 2021 TiKV Project Authors.
>>>>>>> a7e0e74b... [close #489 ] fix backoffer data race (#490)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
<<<<<<< HEAD
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
=======
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
>>>>>>> a7e0e74b... [close #489 ] fix backoffer data race (#490)
 */

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
