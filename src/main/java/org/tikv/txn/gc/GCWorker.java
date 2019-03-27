/*
 * Copyright 2019 The TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.txn.gc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import org.apache.log4j.Logger;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.GCException;
import org.tikv.common.exception.RegionException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.txn.Lock;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class GCWorker implements AutoCloseable {
  private final String uuid;
  private final long l_uuid;
  private final PDClient pdClient;
  private boolean gcIsRunning;
  private long lastFinish;
  private Logger logger = Logger.getLogger(this.getClass());
  private final RegionStoreClientBuilder regionStoreClientBuilder;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
  private final ScheduledExecutorService gcWorkerScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
  private ExecutorService gcTaskThreadPool;
  private ExecutorCompletionService<GCTask> gcTaskService;

  private static final ByteString GC_LAST_RUN_TIME_KEY = ByteString.copyFromUtf8("tikv_gc_last_run_time");
  private static final ByteString GC_RUN_INTERVAL_KEY = ByteString.copyFromUtf8("tikv_gc_run_interval");
  private static final ByteString GC_LIFE_TIME_KEY = ByteString.copyFromUtf8("tikv_gc_life_time");
  private static final ByteString GC_SAFE_POINT_KEY = ByteString.copyFromUtf8("tikv_gc_safe_point");
  private static final ByteString GC_CONCURRENCY_KEY = ByteString.copyFromUtf8("tikv_gc_concurrency");
  private static final ByteString GC_LEADER_UUID_KEY = ByteString.copyFromUtf8("tikv_gc_leader_uuid");
  private static final ByteString GC_LEADER_LEASE_KEY = ByteString.copyFromUtf8("tikv_gc_leader_lease");
  public static final int gcScanLockLimit = 1024;
  private final int gcDefaultRunInterval;
  private final int gcDefaultLifeTime;
  private final int gcDefaultConcurrency;
  private static final int gcMinConcurrency = 1;
  private static final int gcMaxConcurrency = 128;
  private final int gcWaitTime;
  private final int gcWorkerLease;

  public GCWorker(TiSession session) {
    this.pdClient = session.getPDClient();
    TiConfiguration conf = session.getConf();
    this.gcDefaultRunInterval = conf.getGCRunInterval();
    this.gcDefaultLifeTime = conf.getGCLifeTime();
    this.gcDefaultConcurrency = conf.getGCConcurrency();
    this.gcWaitTime = conf.getGCWaitTime();
    this.gcWorkerLease = conf.getGCWorkerLease();
    this.l_uuid = pdClient.grantLease(gcWorkerLease / 1000);
    this.uuid = Long.toHexString(l_uuid);
    this.lastFinish = 0;
    this.regionStoreClientBuilder = new RegionStoreClientBuilder(session.getConf(), session.getChannelFactory(), new RegionManager(session.getPDClient()));
  }

  @Override
  public void close() throws Exception {
    pdClient.unlock(GC_LEADER_UUID_KEY);
    pdClient.revoke(l_uuid);
    if (gcTaskThreadPool != null) {
      gcTaskThreadPool.shutdown();
    }
    scheduler.shutdown();
    gcWorkerScheduler.shutdown();
  }

  private TiTimestamp getTimestamp() {
    return pdClient.getTimestamp(ConcreteBackOffer.newTsoBackOff());
  }

  private long getNow() {
    return getTimestamp().getPhysical();
  }

  // Note: Should not call start() more than once
  public void start() {
    gcWorkerScheduler.schedule(this::run, 0, TimeUnit.MILLISECONDS);
  }

  private void run() {
    logger.info(String.format("[gc worker] %s starts", uuid));
    scheduler.scheduleAtFixedRate(() -> {
      try {
        logger.debug("[gc worker] tick");
        pdClient.keepLeaseAlive(l_uuid);
        tick();
      } catch (Exception e) {
        logger.warn("[gc worker] gc fails to proceed", e);
      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  private void tick() {
    boolean isLeader = checkLeader();
    if (isLeader) {
      try {
        leaderTick();
      } catch (Exception e) {
        logger.warn("[gc worker] check leader err: ", e);
      }
    }
  }

  private void leaderTick() {
    if (gcIsRunning) {
      logger.info(String.format("[gc worker] leader tick on %s: there's already a gc job running. skipped.", uuid));
      return;
    }

    if (getNow() < lastFinish + gcWaitTime) {
      gcIsRunning = false;
      logger.info(String.format("[gc worker] leader tick on %s: another gc job has just finished. skipped.", uuid));
      return;
    }

    long safePoint;
    try {
      safePoint = prepare();
    } catch (Exception e) {
      logger.warn("[gc worker] leader tick err: %s", e);
      return;
    }
    if (safePoint == 0) {
      gcIsRunning = false;
      return;
    }

    gcIsRunning = true;
    logger.info(String.format("[gc worker] %s starts the whole job, safePoint: %d", uuid, safePoint));
    new Thread(() -> runGCJob(safePoint)).start();
  }

  private boolean checkLeader() {
    // acquire lock
    boolean ok = pdClient.lock(GC_LEADER_UUID_KEY, l_uuid) && doCheckLeader();
    // release lock
    pdClient.unlock(GC_LEADER_UUID_KEY);
    return ok;
  }

  private boolean doCheckLeader() {
    long leader = getLong(GC_LEADER_UUID_KEY);
    logger.debug(String.format("[gc worker] got leader: %s", Long.toHexString(leader)));
    if (leader == l_uuid) {
      try {
        putLong(GC_LEADER_LEASE_KEY, getNow() + gcWorkerLease);
      } catch (Exception e) {
        logger.warn("[gc worker] gc leader lease key update fails.", e);
        return false;
      }
      return true;
    }
    long lease = getLong(GC_LEADER_LEASE_KEY);
    long now = getNow();
    if (lease == 0 || lease < now) {
      logger.debug(String.format("[gc worker] register %s as leader", uuid));
      try {
        pdClient.txn().Then(
            toPutOp(GC_LEADER_UUID_KEY, uuid),
            toPutOp(GC_LEADER_LEASE_KEY, now + gcWorkerLease)
        ).commit().get(500, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        logger.warn("[gc worker] gc leader lease key init fails.", e);
        return false;
      }
      return true;
    }
    return false;
  }

  private void runGCJob(long safePoint) {
    try {
      resolveLocks(safePoint);
    } catch (Exception e) {
      logger.error(String.format("[gc worker] %s resolve locks returns an error %s", uuid, e));
      return;
    }
    doGC(safePoint);
  }

  private void doGC(long safePoint) {
    long concurrency = getLongOrElse(GC_CONCURRENCY_KEY, gcDefaultConcurrency);
    if (concurrency < gcMinConcurrency) {
      concurrency = gcMinConcurrency;
    }
    if (concurrency > gcMaxConcurrency) {
      concurrency = gcMaxConcurrency;
    }

    doGCInternal(safePoint, concurrency);
  }

  private void doGCInternal(long safePoint, long concurrency) {
    putLong(GC_SAFE_POINT_KEY, safePoint);
    logger.info(String.format("[gc worker] %s start gc, concurrency %d, safePoint: %d.", uuid, concurrency, safePoint));

    try {
      gcTaskThreadPool = Executors.newFixedThreadPool((int) concurrency);
      gcTaskService = new ExecutorCompletionService<>(gcTaskThreadPool);
      for (int i = 0; i < concurrency; i++) {
        new Thread(() -> newGCTaskWorker(uuid).run()).start();
      }
      ByteString key = ByteString.EMPTY;
      while (true) {
        BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.GcOneRegionMaxBackoff);
        GCTask task = genNextGCTask(backOffer, safePoint, key);
        gcTaskService.submit(() -> task);
        key = task.endKey;
        if (key.equals(ByteString.EMPTY)) {
          awaitGCComplete();
          return;
        }
      }
    } catch (Exception e) {
      awaitGCComplete();
    }
  }

  private void awaitGCComplete() {
    try {
      if (gcTaskThreadPool != null) {
        gcTaskThreadPool.awaitTermination(gcWorkerLease, TimeUnit.MILLISECONDS);
      }
      lastFinish = getNow();
      logger.info("[gc worker] gc job complete");
    } catch (InterruptedException e) {
      logger.error("[gc worker] gc aborted with expired lease");
    }
    gcIsRunning = false;
  }

  private class GCTask {
    private final ByteString startKey;
    private final ByteString endKey;
    private final long safePoint;

    GCTask(ByteString startKey, ByteString endKey, long safePoint) {
      this.startKey = startKey;
      this.endKey = endKey;
      this.safePoint = safePoint;
    }
  }

  private class GCTaskWorker {
    String identifier;

    GCTaskWorker(String uuid) {
      this.identifier = uuid;
    }

    void run() {
      GCTask task;
      try {
        logger.info("[gc worker] run gc task worker");
        while (true) {
          Future<GCTask> gcTaskFuture;
          if ((gcTaskFuture = gcTaskService.take()) == null) {
            logger.info("[gc worker] No tasks remain");
            return;
          }
          task = gcTaskFuture.get();
          if (logger.isDebugEnabled()) {
            logger.debug("[gc worker] receive " + KeyUtils.formatBytes(task.startKey) + " " + KeyUtils.formatBytes(task.endKey) + " " + task.safePoint);
          }
          try {
            doGCForRange(task.startKey, task.endKey, task.safePoint);
          } catch (Exception e) {
            logger.error(String.format("[gc worker] %s, gc interrupted because get region(%s, %s) error, err %s",
                identifier, task.startKey.toStringUtf8(), task.endKey.toStringUtf8(), e));
            return;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("[gc worker] Current thread interrupted.", e);
      } catch (ExecutionException e) {
        logger.warn("[gc worker] Execution exception met.", e);
      }
    }

    private void doGCForRange(ByteString startKey, ByteString endKey, long safePoint) {
      ByteString key = startKey;
      while (true) {
        BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.GcOneRegionMaxBackoff);
        TiRegion region = pdClient.getRegionByKey(backOffer, key);
        try {
          doGCForRegion(backOffer, safePoint, region);
        } catch (RegionException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        } catch (Exception e) {
          logger.warn(String.format("[gc worker] %s gc for range [%s, %s) safepoint: %d, failed", identifier, KeyUtils.formatBytes(startKey), KeyUtils.formatBytes(endKey), safePoint), e);
        }
        key = region.getEndKey();
        if (key.equals(ByteString.EMPTY) || (!endKey.equals(ByteString.EMPTY) && FastByteComparisons.compareTo(key.toByteArray(), endKey.toByteArray()) >= 0)) {
          if (logger.isDebugEnabled()) {
            logger.debug(String.format("[gc worker] doGCForRange [%s, %s) complete.", KeyUtils.formatBytes(startKey), KeyUtils.formatBytes(endKey)));
          }
          return;
        }
      }
    }

    private void doGCForRegion(BackOffer backOffer, long safePoint, TiRegion region) {
      RegionStoreClient client = regionStoreClientBuilder.build(region);
      logger.debug("[gc worker] doGCForRegion");
      client.doGC(backOffer, safePoint);
      logger.debug("[gc worker] ~doGCForRegion");
    }
  }

  private GCTaskWorker newGCTaskWorker(String uuid) {
    return new GCTaskWorker(uuid);
  }

  private GCTask genNextGCTask(BackOffer backOffer, long safePoint, ByteString key) {
    ByteString endKey = pdClient.getRegionByKey(backOffer, key).getEndKey();
    return new GCTask(key, endKey, safePoint);
  }

  private long prepare() {
    long now = getNow();
    boolean ok = checkGCInterval(now);
    if (!ok) {
      return 0;
    }
    // calculate new safe point
    long newSafePoint = calculateNewSafePoint(now);
    if (newSafePoint != 0) {
      // save last run time
      putLong(GC_LAST_RUN_TIME_KEY, now);
      // save new safe point
      putLong(GC_SAFE_POINT_KEY, newSafePoint);
    }
    return newSafePoint;
  }

  private boolean checkGCInterval(long now) {
    long runInterval = loadAndSaveLongWithDefault(GC_RUN_INTERVAL_KEY, gcDefaultRunInterval);
    long lastRun = getLong(GC_LAST_RUN_TIME_KEY);

    if (lastRun != 0 && lastRun + runInterval > now) {
      logger.info(String.format("[gc worker] leaderTick on %s: until now (%d), gc interval (%d) haven't past since last run (%d). no need to gc", uuid, now, runInterval, lastRun));
      return false;
    }
    return true;
  }

  private long calculateNewSafePoint(long now) {
    long lifeTime = loadAndSaveLongWithDefault(GC_LIFE_TIME_KEY, gcDefaultLifeTime);
    long lastSafePoint = getLong(GC_SAFE_POINT_KEY);
    long safePoint = now - lifeTime;
    if (safePoint < lastSafePoint) {
      logger.info(String.format("[gc worker] leaderTick on %s: last safe point (%d) is later than current one (%d). no need to gc. " +
          "this might be caused by manually enlarging gc lifetime.", uuid, lastSafePoint, safePoint));
      return 0;
    }
    return safePoint;
  }

  private long loadAndSaveLongWithDefault(ByteString key, long defaultValue) {
    long d = getLong(key);
    if (d == 0) {
      putLong(key, defaultValue);
      return defaultValue;
    }
    return d;
  }

  private ByteString get(ByteString key) {
    return pdClient.get(key);
  }

  private ByteString getOrThrow(ByteString key) {
    ByteString value = get(key);
    if (value == null) {
      throw new RuntimeException("Value for " + key.toStringUtf8() + " not found in pd.");
    }
    return value;
  }

  private long getLongOrElse(ByteString key, long defalutValue) {
    ByteString value = get(key);
    if (value == null) {
      return defalutValue;
    }
    return Long.valueOf(value.toStringUtf8());
  }

  private long getLongOrThrow(ByteString key) {
    ByteString value = getOrThrow(key);
    return Long.valueOf(value.toStringUtf8());
  }

  private long getLong(ByteString key) {
    return getLongOrElse(key, 0);
  }

  private String getString(ByteString key) {
    ByteString value = get(key);
    if (value == null) {
      return null;
    }
    return value.toStringUtf8();
  }

  private void putLong(ByteString key, long value) {
    pdClient.put(key, ByteString.copyFromUtf8(String.valueOf(value)));
  }

  private void putString(ByteString key, String value) {
    pdClient.put(key, ByteString.copyFromUtf8(value));
  }


  private static Op toPutOp(ByteString key, String value) {
    return Op.put(
        ByteSequence.from(key),
        ByteSequence.from(value.getBytes()),
        PutOption.DEFAULT
    );
  }

  private static Op toPutOp(ByteString key, long value) {
    return toPutOp(key, String.valueOf(value));
  }

  private void resolveLocks(long safePoint) {
    Kvrpcpb.ScanLockRequest.Builder requestBuilder = Kvrpcpb.ScanLockRequest.newBuilder().setMaxVersion(safePoint).setLimit(gcScanLockLimit);
    ByteString key = ByteString.EMPTY;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.GcResolveLockMaxBackoff);
    int totalResolvedLocks = 0, regions = 0;
    long startTime = System.currentTimeMillis();
    while (true) {
      requestBuilder.setStartKey(key);
      TiRegion region = pdClient.getRegionByKey(backOffer, key);
      RegionStoreClient client = regionStoreClientBuilder.build(region);
      List<Kvrpcpb.LockInfo> locksInfo = client.scanLocks(backOffer, key, safePoint);
      List<Lock> locks = locksInfo.stream().map(Lock::new).collect(Collectors.toList());
      boolean ok = client.getLockResolverClient().resolveLocks(backOffer, locks);
      if (!ok) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoTxnLock, new GCException("Remaining locks: " + locks.size()));
      }
      totalResolvedLocks += locks.size();
      if (locks.size() < gcScanLockLimit) {
        ++regions;
        key = region.getEndKey();
        if (key == null || key.equals(ByteString.EMPTY)) {
          break;
        }
      } else {
        logger.info(String.format("[gc worker] %s, region %d has more than %d locks", this.uuid, region.getId(), gcScanLockLimit));
        key = locks.get(locks.size() - 1).getKey();
      }
    }
    logger.info(String.format("[gc worker] %s finish resolve locks, safePoint: %s, regions: %s, total resolved: %d, cost time: %f s", uuid, safePoint, regions, totalResolvedLocks, (System.currentTimeMillis() - startTime) / 1000.0));
  }

}
