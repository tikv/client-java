package org.tikv.common.operation;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.region.RegionErrorReceiver;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Metapb;

public class RegionErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = LoggerFactory.getLogger(RegionErrorHandler.class);
  // if a store does not have leader currently, store id is set to 0
  private static final int NO_LEADER_STORE_ID = 0;
  private final Function<RespT, Errorpb.Error> getRegionError;
  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;

  public RegionErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.recv = recv;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
  }

  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      String msg = String.format("Request Failed with unknown reason for [%s]", recv.getRegion());
      return handleRequestError(backOffer, new GrpcException(msg));
    }
    // Region error handling logic
    Errorpb.Error error = getRegionError(resp);
    if (error != null) {
      return handleRegionError(backOffer, error);
    }
    return false;
  }

  public boolean handleRegionError(BackOffer backOffer, Errorpb.Error error) {
    if (error.hasNotLeader()) {
      // this error is reported from raftstore:
      // peer of current request is not leader, the following might be its causes:
      // 1. cache is outdated, region has changed its leader, can be solved by re-fetching from PD
      // 2. leader of current region is missing, need to wait and then fetch region info from PD
      long newStoreId = error.getNotLeader().getLeader().getStoreId();
      boolean retry;

      // update Leader here
      logger.warn(
          String.format(
              "NotLeader Error with region id %d and store id %d, new store id %d",
              recv.getRegion().getId(), recv.getRegion().getLeader().getStoreId(), newStoreId));

      BackOffFunction.BackOffFuncType backOffFuncType;
      // if there's current no leader, we do not trigger update pd cache logic
      // since issuing store = NO_LEADER_STORE_ID requests to pd will definitely fail.
      if (newStoreId != NO_LEADER_STORE_ID) {
        // If update leader fails, we need to fetch new region info from pd,
        // and re-split key range for new region. Setting retry to false will
        // stop retry and enter handleCopResponse logic, which would use RegionMiss
        // backOff strategy to wait, fetch new region and re-split key range.
        // onNotLeader is only needed when updateLeader succeeds, thus switch
        // to a new store address.
        TiRegion newRegion = this.regionManager.updateLeader(recv.getRegion(), newStoreId);
        retry = newRegion != null && recv.onNotLeader(newRegion);

        backOffFuncType = BackOffFunction.BackOffFuncType.BoUpdateLeader;
      } else {
        logger.info(
            String.format(
                "Received zero store id, from region %d try next time", recv.getRegion().getId()));

        backOffFuncType = BackOffFunction.BackOffFuncType.BoRegionMiss;
        retry = false;
      }

      if (!retry) {
        this.regionManager.invalidateRegion(recv.getRegion());
      }

      backOffer.doBackOff(backOffFuncType, new GrpcException(error.toString()));

      return retry;
    } else if (error.hasStoreNotMatch()) {
      // this error is reported from raftstore:
      // store_id requested at the moment is inconsistent with that expected
      // Solution：re-fetch from PD
      long storeId = recv.getRegion().getLeader().getStoreId();
      long actualStoreId = error.getStoreNotMatch().getActualStoreId();
      logger.warn(
          String.format(
              "Store Not Match happened with region id %d, store id %d, actual store id %d",
              recv.getRegion().getId(), storeId, actualStoreId));

      this.regionManager.invalidateRegion(recv.getRegion());
      this.regionManager.invalidateStore(storeId);
      // recv.onStoreNotMatch(this.regionManager.getStoreById(storeId));
      // assume this is a low probability error, do not retry, just re-split the request by
      // throwing it out.
      return false;
    } else if (error.hasEpochNotMatch()) {
      logger.warn(
          String.format("tikv reports `EpochNotMatch` retry later, region: %s", recv.getRegion()));
      return onRegionEpochNotMatch(backOffer, error.getEpochNotMatch().getCurrentRegionsList());
    } else if (error.hasServerIsBusy()) {
      // this error is reported from kv:
      // will occur when write pressure is high. Please try later.
      logger.warn(
          String.format(
              "Server is busy for region [%s], reason: %s",
              recv.getRegion(), error.getServerIsBusy().getReason()));
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoServerBusy,
          new StatusRuntimeException(
              Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString())));
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
      return true;
    } else if (error.hasStaleCommand()) {
      // this error is reported from raftstore:
      // command outdated, please try later
      logger.warn(String.format("Stale command for region [%s]", recv.getRegion()));
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
      return true;
    } else if (error.hasRaftEntryTooLarge()) {
      logger.warn(String.format("Raft too large for region [%s]", recv.getRegion()));
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
              recv.getRegion(), KeyUtils.formatBytesUTF8(invalidKey)));
      regionManager.clearRegionCache();
      throw new StatusRuntimeException(Status.UNKNOWN.withDescription(error.toString()));
    }

    logger.warn(String.format("Unknown error %s for region [%s]", error, recv.getRegion()));
    // For other errors, we only drop cache here.
    // Upper level may split this task.
    invalidateRegionStoreCache(recv.getRegion());
    // retry if raft proposal is dropped, it indicates the store is in the middle of transition
    if (error.getMessage().contains("Raft ProposalDropped")) {
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
      return true;
    }
    return false;
  }

  // ref: https://github.com/tikv/client-go/blob/tidb-5.2/internal/locate/region_request.go#L985
  // OnRegionEpochNotMatch removes the old region and inserts new regions into the cache.
  // It returns whether retries the request because it's possible the region epoch is ahead of
  // TiKV's due to slow appling.
  private boolean onRegionEpochNotMatch(BackOffer backOffer, List<Metapb.Region> currentRegions) {
    if (currentRegions.size() == 0) {
      this.regionManager.onRegionStale(recv.getRegion());
      return false;
    }

    // Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
    for (Metapb.Region meta : currentRegions) {
      if (meta.getId() == recv.getRegion().getId()
          && (meta.getRegionEpoch().getConfVer() < recv.getRegion().getVerID().getConfVer()
              || meta.getRegionEpoch().getVersion() < recv.getRegion().getVerID().getVer())) {
        String errorMsg =
            String.format(
                "region epoch is ahead of tikv, region: %s, currentRegions: %s",
                recv.getRegion(), currentRegions);
        logger.info(errorMsg);
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new TiKVException(errorMsg));
        return true;
      }
    }

    boolean needInvalidateOld = true;
    List<TiRegion> newRegions = new ArrayList<>(currentRegions.size());
    // If the region epoch is not ahead of TiKV's, replace region meta in region cache.
    for (Metapb.Region meta : currentRegions) {
      TiRegion region = regionManager.createRegion(meta, backOffer);
      newRegions.add(region);
      if (recv.getRegion().getVerID() == region.getVerID()) {
        needInvalidateOld = false;
      }
    }

    if (needInvalidateOld) {
      this.regionManager.onRegionStale(recv.getRegion());
    }

    for (TiRegion region : newRegions) {
      regionManager.insertRegionToCache(region);
    }

    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    if (recv.onStoreUnreachable(backOffer.getSlowLog())) {
      if (!backOffer.canRetryAfterSleep(BackOffFunction.BackOffFuncType.BoTiKVRPC)) {
        regionManager.onRequestFail(recv.getRegion());
        throw new GrpcException("retry is exhausted.", e);
      }
      return true;
    }

    logger.warn("request failed because of: " + e.getMessage());
    if (!backOffer.canRetryAfterSleep(BackOffFunction.BackOffFuncType.BoTiKVRPC)) {
      regionManager.onRequestFail(recv.getRegion());
      throw new GrpcException(
          "send tikv request error: " + e.getMessage() + ", try next peer later", e);
    }
    // TiKV maybe down, so do not retry in `callWithRetry`
    // should re-fetch the new leader from PD and send request to it
    return false;
  }

  public Errorpb.Error getRegionError(RespT resp) {
    if (getRegionError != null) {
      return getRegionError.apply(resp);
    }
    return null;
  }

  public TiRegion getRegion() {
    return recv.getRegion();
  }

  private void invalidateRegionStoreCache(TiRegion ctxRegion) {
    regionManager.invalidateRegion(ctxRegion);
    regionManager.invalidateStore(ctxRegion.getLeader().getStoreId());
  }
}
