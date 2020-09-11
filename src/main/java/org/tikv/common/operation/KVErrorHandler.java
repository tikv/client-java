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

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Function;
import org.apache.log4j.Logger;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.region.RegionErrorReceiver;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.kvproto.Errorpb;

// TODO: consider refactor to Builder mode
public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = Logger.getLogger(KVErrorHandler.class);
  private static final int NO_LEADER_STORE_ID =
      0; // if there's currently no leader of a store, store id is set to 0
  private final Function<RespT, Errorpb.Error> getRegionError;
  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;
  private final TiRegion ctxRegion;

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      TiRegion ctxRegion,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.ctxRegion = ctxRegion;
    this.recv = recv;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
  }

  private Errorpb.Error getRegionError(RespT resp) {
    if (getRegionError != null) {
      return getRegionError.apply(resp);
    }
    return null;
  }

  private void invalidateRegionStoreCache(TiRegion ctxRegion) {
    regionManager.invalidateRegion(ctxRegion.getId());
    regionManager.invalidateStore(ctxRegion.getLeader().getStoreId());
  }

  // Referenced from TiDB
  // store/tikv/region_request.go - onRegionError
  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      String msg =
          String.format("Request Failed with unknown reason for region region [%s]", ctxRegion);
      logger.warn(msg);
      return handleRequestError(backOffer, new GrpcException(msg));
    }

    // Region error handling logic
    Errorpb.Error error = getRegionError(resp);
    if (error != null) {
      if (error.hasNotLeader()) {
        // this error is reported from raftstore:
        // peer of current request is not leader, the following might be its causes:
        // 1. cache is outdated, region has changed its leader, can be solved by re-fetching from PD
        // 2. leader of current region is missing, need to wait and then fetch region info from PD
        long newStoreId = error.getNotLeader().getLeader().getStoreId();
        boolean retry = true;

        // update Leader here
        logger.warn(
            String.format(
                "NotLeader Error with region id %d and store id %d, new store id %d",
                ctxRegion.getId(), ctxRegion.getLeader().getStoreId(), newStoreId));

        // if there's current no leader, we do not trigger update pd cache logic
        // since issuing store = NO_LEADER_STORE_ID requests to pd will definitely fail.
        if (newStoreId != NO_LEADER_STORE_ID) {
          if (!this.regionManager.checkAndDropLeader(ctxRegion.getId(), newStoreId)
              || !recv.onNotLeader(this.regionManager.getStoreById(newStoreId))) {
            // If update leader fails, we need to fetch new region info from pd,
            // and re-split key range for new region. Setting retry to false will
            // stop retry and enter handleCopResponse logic, which would use RegionMiss
            // backOff strategy to wait, fetch new region and re-split key range.
            // onNotLeader is only needed when checkAndDropLeader succeeds, thus switch
            // to a new store address.
            retry = false;
          }
          // https://github.com/tikv/tikv/issues/7941
          // If update leader returns a valid leader store id, don't do back off

        } else {
          logger.info(
              String.format(
                  "Received zero store id, from region %d try next time", ctxRegion.getId()));
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.toString()));
        }
        return retry;
      } else if (error.hasStoreNotMatch()) {
        // this error is reported from raftstore:
        // store_id requested at the moment is inconsistent with that expected
        // Solution：re-fetch from PD
        long storeId = ctxRegion.getLeader().getStoreId();
        logger.warn(
            String.format(
                "Store Not Match happened with region id %d, store id %d",
                ctxRegion.getId(), storeId));
        logger.warn(String.format("%s", error.getStoreNotMatch()));

        this.regionManager.invalidateStore(storeId);
        recv.onStoreNotMatch(this.regionManager.getStoreById(storeId));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoStoreNotMatch, new GrpcException(error.toString()));
        return true;
      } else if (error.hasEpochNotMatch()) {
        // this error is reported from raftstore:
        // region has outdated version，please try later.
        logger.warn(String.format("Stale Epoch encountered for region [%s]", ctxRegion));
        this.regionManager.onRegionStale(ctxRegion.getId());
        return false;
      } else if (error.hasServerIsBusy()) {
        // this error is reported from kv:
        // will occur when write pressure is high. Please try later.
        logger.warn(
            String.format(
                "Server is busy for region [%s], reason: %s",
                ctxRegion, error.getServerIsBusy().getReason()));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoServerBusy,
            new StatusRuntimeException(
                Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString())));
        return true;
      } else if (error.hasStaleCommand()) {
        // this error is reported from raftstore:
        // command outdated, please try later
        logger.warn(String.format("Stale command for region [%s]", ctxRegion));
        return true;
      } else if (error.hasRaftEntryTooLarge()) {
        logger.warn(String.format("Raft too large for region [%s]", ctxRegion));
        throw new StatusRuntimeException(
            Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasKeyNotInRegion()) {
        // this error is reported from raftstore:
        // key requested is not in current region
        // should not happen here.
        ByteString invalidKey = error.getKeyNotInRegion().getKey();
        logger.error(
            String.format(
                "Key not in region [%s] for key [%s], this error should not happen here.",
                ctxRegion, KeyUtils.formatBytes(invalidKey)));
        throw new StatusRuntimeException(Status.UNKNOWN.withDescription(error.toString()));
      }

      logger.warn(String.format("Unknown error for region [%s]", ctxRegion));
      // For other errors, we only drop cache here.
      // Upper level may split this task.
      invalidateRegionStoreCache(ctxRegion);
    }

    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    regionManager.onRequestFail(ctxRegion);

    backOffer.doBackOff(
        BackOffFunction.BackOffFuncType.BoTiKVRPC,
        new GrpcException(
            "send tikv request error: " + e.getMessage() + ", try next peer later", e));
    return true;
  }
}
