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

package org.tikv.common.util;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Kvrpcpb;

public class ClientUtils {
  /**
   * Append batch to list and split them according to batch limit
   *
   * @param batches a grouped batch
   * @param region region
   * @param keys keys
   * @param batchMaxSizeInBytes batch max limit
   */
  public static void appendBatches(
      List<Batch> batches, TiRegion region, List<ByteString> keys, int batchMaxSizeInBytes) {
    if (keys == null) {
      return;
    }
    int len = keys.size();
    for (int start = 0, end; start < len; start = end) {
      int size = 0;
      for (end = start; end < len && size < batchMaxSizeInBytes; end++) {
        size += keys.get(end).size();
      }
      Batch batch = new Batch(region, keys.subList(start, end));
      batches.add(batch);
    }
  }

  /**
   * Append batch to list and split them according to batch limit
   *
   * @param batches a grouped batch
   * @param region region
   * @param keys keys
   * @param values values
   * @param batchMaxSizeInBytes batch max limit
   */
  public static void appendBatches(
      List<Batch> batches,
      TiRegion region,
      List<ByteString> keys,
      List<ByteString> values,
      int batchMaxSizeInBytes) {
    if (keys == null) {
      return;
    }
    int len = keys.size();
    for (int start = 0, end; start < len; start = end) {
      int size = 0;
      for (end = start; end < len && size < batchMaxSizeInBytes; end++) {
        size += keys.get(end).size();
        size += values.get(end).size();
      }
      Batch batch = new Batch(region, keys.subList(start, end), values.subList(start, end));
      batches.add(batch);
    }
  }

  public static List<Kvrpcpb.KvPair> getKvPairs(
      ExecutorCompletionService<List<Kvrpcpb.KvPair>> completionService,
      List<Batch> batches,
      int backOff) {
    try {
      List<Kvrpcpb.KvPair> result = new ArrayList<>();
      for (int i = 0; i < batches.size(); i++) {
        result.addAll(completionService.take().get(backOff, TimeUnit.MILLISECONDS));
      }
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (TimeoutException e) {
      throw new TiKVException("TimeOut Exceeded for current operation. ", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
  }

  public static void getTasks(
      ExecutorCompletionService<List<Batch>> completionService,
      Queue<List<Batch>> taskQueue,
      List<Batch> batches,
      int backOff) {
    try {
      for (int i = 0; i < batches.size(); i++) {
        List<Batch> task = completionService.take().get(backOff, TimeUnit.MILLISECONDS);
        if (!task.isEmpty()) {
          taskQueue.offer(task);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (TimeoutException e) {
      throw new TiKVException("TimeOut Exceeded for current operation. ", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
  }
}
