/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package org.tikv.common.operation;

import static org.tikv.common.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import java.util.Collections;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.KeyException;
import org.tikv.common.region.RegionErrorReceiver;
import org.tikv.common.region.RegionManager;
import org.tikv.common.util.BackOffer;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.txn.AbstractLockResolverClient;
import org.tikv.txn.Lock;
import org.tikv.txn.ResolveLockResult;

// TODO: consider refactor to Builder mode
public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = LoggerFactory.getLogger(KVErrorHandler.class);
  private final Function<RespT, Kvrpcpb.KeyError> getKeyError;
  private final Function<ResolveLockResult, Object> resolveLockResultCallback;
  private final AbstractLockResolverClient lockResolverClient;
  private final long callerStartTS;
  private final boolean forWrite;
  private final RegionErrorHandler<RespT> regionHandler;

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      AbstractLockResolverClient lockResolverClient,
      Function<RespT, Errorpb.Error> getRegionError,
      Function<RespT, Kvrpcpb.KeyError> getKeyError,
      Function<ResolveLockResult, Object> resolveLockResultCallback,
      long callerStartTS,
      boolean forWrite) {
    this.regionHandler = new RegionErrorHandler<>(regionManager, recv, getRegionError);
    this.lockResolverClient = lockResolverClient;
    this.getKeyError = getKeyError;
    this.resolveLockResultCallback = resolveLockResultCallback;
    this.callerStartTS = callerStartTS;
    this.forWrite = forWrite;
  }

  private void resolveLock(BackOffer backOffer, Lock lock) {
    if (lockResolverClient != null) {
      logger.warn("resolving lock");

      ResolveLockResult resolveLockResult =
          lockResolverClient.resolveLocks(
              backOffer, callerStartTS, Collections.singletonList(lock), forWrite);
      resolveLockResultCallback.apply(resolveLockResult);
      long msBeforeExpired = resolveLockResult.getMsBeforeTxnExpired();
      if (msBeforeExpired > 0) {
        // if not resolve all locks, we wait and retry
        backOffer.doBackOffWithMaxSleep(
            BoTxnLockFast, msBeforeExpired, new KeyException(lock.toString()));
      }
    }
  }

  // Referenced from TiDB
  // store/tikv/region_request.go - onRegionError

  /** @return true: client should retry */
  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      String msg =
          String.format("Request Failed with unknown reason for [%s]", regionHandler.getRegion());
      logger.warn(msg);
      return handleRequestError(backOffer, new GrpcException(msg));
    }

    Errorpb.Error error = regionHandler.getRegionError(resp);
    if (error != null) {
      return regionHandler.handleRegionError(backOffer, error);
    }

    // Key error handling logic
    Kvrpcpb.KeyError keyError = getKeyError.apply(resp);
    if (keyError != null) {
      try {
        Lock lock = AbstractLockResolverClient.extractLockFromKeyErr(keyError);
        resolveLock(backOffer, lock);
        return true;
      } catch (KeyException e) {
        logger.warn("Unable to handle KeyExceptions other than LockException", e);
      }
    }
    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    return regionHandler.handleRequestError(backOffer, e);
  }
}
