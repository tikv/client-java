package org.tikv.common.region;

import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb;

public class StoreHealthyChecker implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(StoreHealthyChecker.class);
  private static final long MAX_CHECK_STORE_TOMBSTONE_TICK = 60;
  private static final long SLEEP_MILLI_SECONDS_AFTER_DOUBLE_CHECK = 500;
  private BlockingQueue<TiStore> taskQueue;
  private final ChannelFactory channelFactory;
  private final ReadOnlyPDClient pdClient;
  private final RegionCache cache;
  private long checkTombstoneTick;

  public StoreHealthyChecker(
      ChannelFactory channelFactory, ReadOnlyPDClient pdClient, RegionCache cache) {
    this.taskQueue = new LinkedBlockingQueue<>();
    this.channelFactory = channelFactory;
    this.pdClient = pdClient;
    this.cache = cache;
    this.checkTombstoneTick = 0;
  }

  public boolean scheduleStoreHealthCheck(TiStore store) {
    if (!this.taskQueue.add(store)) {
      // add queue false, mark it reachable so that it can be put again.
      return false;
    }
    return true;
  }

  private List<TiStore> getValidStores() {
    List<TiStore> unhealthStore = new LinkedList<>();
    while (!this.taskQueue.isEmpty()) {
      try {
        TiStore store = this.taskQueue.take();
        if (!store.isValid()) {
          continue;
        }
        unhealthStore.add(store);
      } catch (Exception e) {
        return unhealthStore;
      }
    }
    return unhealthStore;
  }

  private boolean checkStoreHealth(TiStore store) {
    String addressStr = store.getStore().getAddress();
    try {
      ManagedChannel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
      HealthGrpc.HealthBlockingStub stub =
          HealthGrpc.newBlockingStub(channel).withDeadlineAfter(200, TimeUnit.MILLISECONDS);
      HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
      HealthCheckResponse resp = stub.check(req);
      if (resp.getStatus() == HealthCheckResponse.ServingStatus.SERVING) {
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  private boolean checkStoreTombstone(TiStore store) {
    try {
      Metapb.Store newStore = pdClient.getStore(ConcreteBackOffer.newRawKVBackOff(), store.getId());
      if (newStore.getState() == Metapb.StoreState.Tombstone) {
        return true;
      }
    } catch (Exception e) {
      return false;
    }
    return false;
  }

  @Override
  public void run() {
    checkTombstoneTick += 1;
    boolean needCheckTombstoneStore = false;
    if (checkTombstoneTick == MAX_CHECK_STORE_TOMBSTONE_TICK) {
      needCheckTombstoneStore = true;
    }
    List<TiStore> allStores = getValidStores();
    List<TiStore> unreachableStore = new LinkedList<>();
    for (TiStore store : allStores) {
      if (needCheckTombstoneStore && checkStoreTombstone(store)) {
        store.markInvalid();
        continue;
      }
      if (checkStoreHealth(store)) {
        if (store.getProxyStore() != null) {
          TiStore newStore = store.withProxy(null);
          if (cache.putStore(newStore.getId(), newStore)) {
            store.markInvalid();
          } else {
            newStore.markInvalid();
          }
          logger.warn(
              String.format("store [%s] recovers to be reachable", store.getStore().getAddress()));
        } else if (!store.isReachable()) {
          store.markReachable();
        }
      } else if (store.isReachable()) {
        unreachableStore.add(store);
        continue;
      }
      this.taskQueue.add(store);
    }
    if (!unreachableStore.isEmpty()) {
      try {
        Thread.sleep(SLEEP_MILLI_SECONDS_AFTER_DOUBLE_CHECK);
      } catch (Exception e) {
        this.taskQueue.addAll(unreachableStore);
        return;
      }
      for (TiStore store : unreachableStore) {
        if (!checkStoreHealth(store)) {
          store.markUnreachable();
        }
        this.taskQueue.add(store);
      }
    }
  }
}
