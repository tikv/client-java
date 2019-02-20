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
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import org.apache.log4j.Logger;
import org.tikv.common.PDClient;
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

import static org.tikv.txn.gc.GCWorker.GCWorkerConst.*;

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

  public GCWorker(TiSession session) {
    this.pdClient = session.getPDClient();
    this.l_uuid = pdClient.grantLease(gcWorkerLease / 1000);
    this.uuid = Long.toHexString(l_uuid);
    logger.info("uuid = " + uuid + " " + l_uuid);
    this.lastFinish = 0;
    this.regionStoreClientBuilder = new RegionStoreClientBuilder(session.getConf(), session.getChannelFactory(), new RegionManager(session.getPDClient()));
  }

  @Override
  public void close() throws Exception {
    pdClient.unlock(gcLeaderUUIDKey);
    pdClient.revoke(l_uuid);
    if (gcTaskThreadPool != null) {
      gcTaskThreadPool.shutdown();
    }
    scheduler.shutdown();
    gcWorkerScheduler.shutdown();
  }

  public static class GCWorkerConst {
    static final ByteString gcLastRunTimeKey = ByteString.copyFromUtf8("tikv_gc_last_run_time");
    static final ByteString gcRunIntervalKey = ByteString.copyFromUtf8("tikv_gc_run_interval");
    static final ByteString gcLifeTimeKey = ByteString.copyFromUtf8("tikv_gc_life_time");
    static final ByteString gcSafePointKey = ByteString.copyFromUtf8("tikv_gc_safe_point");
    static final ByteString gcConcurrencyKey = ByteString.copyFromUtf8("tikv_gc_concurrency");
    static final ByteString gcLeaderUUIDKey = ByteString.copyFromUtf8("tikv_gc_leader_uuid");
    static final ByteString gcLeaderLeaseKey = ByteString.copyFromUtf8("tikv_gc_leader_lease");
    static final int gcDefaultRunInterval = 10 * 60 * 1000;
    static final int gcDefaultLifeTime = 2 * 1000;//10 * 60 * 1000;
    static final int gcDefaultConcurrency = 2;
    static final int gcWaitTime = 3 * 1000;//60 * 1000;
    public static final int gcScanLockLimit = 1024;
    static final int gcMinConcurrency = 1;
    static final int gcMaxConcurrency = 128;
    static final int gcWorkerLease = 2 * 1000;//2 * 60 * 1000;
  }

  private TiTimestamp getTimestamp() {
    return pdClient.getTimestamp(ConcreteBackOffer.newTsoBackOff());
  }

  // Note: Should not call run() more than once
  public void run() {
    gcWorkerScheduler.schedule(this::start, 0, TimeUnit.MILLISECONDS);
  }

  public void start() {
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

    if (getTimestamp().getPhysical() < lastFinish + gcWaitTime) {
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
    runGCJob(safePoint);
    lastFinish = System.currentTimeMillis();
    gcIsRunning = false;
  }

  private boolean checkLeader() {
    // acquire lock
    boolean ok = pdClient.lock(gcLeaderUUIDKey, l_uuid) && doCheckLeader();
    // release lock
    pdClient.unlock(gcLeaderUUIDKey);
    return ok;
  }

  private boolean doCheckLeader() {
    long leader = getLong(gcLeaderUUIDKey);
    logger.debug(String.format("[gc worker] got leader: %s", Long.toHexString(leader)));
    if (leader == l_uuid) {
      try {
        putLong(gcLeaderLeaseKey, System.currentTimeMillis() + gcWorkerLease);
      } catch (Exception e) {
        return false;
      }
      return true;
    }
    long lease = getLong(gcLeaderLeaseKey);
    if (lease == 0 || lease < System.currentTimeMillis()) {
      logger.debug(String.format("[gc worker] register %s as leader", uuid));
      try {
        pdClient.txn().Then(
            toPutOp(gcLeaderUUIDKey, uuid),
            toPutOp(gcLeaderLeaseKey, System.currentTimeMillis() + gcWorkerLease)
        ).commit().get(500, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
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
    long concurrency = getLongOrElse(gcConcurrencyKey, gcDefaultConcurrency);
    if (concurrency < gcMinConcurrency) {
      concurrency = gcMinConcurrency;
    }
    if (concurrency > gcMaxConcurrency) {
      concurrency = gcMaxConcurrency;
    }

    doGCInternal(safePoint, concurrency);
  }

  private void doGCInternal(long safePoint, long concurrency) {
    putLong(gcSafePointKey, safePoint);
    logger.info(String.format("[gc worker] %s start gc, concurrency %d, safePoint: %d.", uuid, concurrency, safePoint));

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
        return;
      }
    }
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
            logger.error("[gc worker] No tasks remain");
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
        if (key.equals(ByteString.EMPTY) || FastByteComparisons.compareTo(key.toByteArray(), endKey.toByteArray()) >= 0) {
          logger.info("[gc worker] doGCForRange complete.");
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
    long now = getTimestamp().getPhysical();
    boolean ok = checkGCInterval(now);
    if (!ok) {
      return 0;
    }
    // calculate new safe point
    long newSafePoint = calculateNewSafePoint(now);
    if (newSafePoint != 0) {
      // save last run time
      putLong(gcLastRunTimeKey, now);
      // save new safe point
      putLong(gcSafePointKey, newSafePoint);
    }
    return newSafePoint;
  }

  private boolean checkGCInterval(long now) {
//    long runInterval = loadAndSaveLongWithDefault(gcRunIntervalKey, gcDefaultRunInterval);
    long runInterval = 6 * 1000;
    long lastRun = getLong(gcLastRunTimeKey);

    if (lastRun != 0 && lastRun + runInterval > now) {
      logger.info(String.format("[gc worker] leaderTick on %s: until now (%d), gc interval (%d) haven't past since last run (%d). no need to gc", uuid, now, runInterval, lastRun));
      return false;
    }
    return true;
  }

  private long calculateNewSafePoint(long now) {
    long lifeTime = loadAndSaveLongWithDefault(gcLifeTimeKey, gcDefaultLifeTime);
    long lastSafePoint = getLong(gcSafePointKey);
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
