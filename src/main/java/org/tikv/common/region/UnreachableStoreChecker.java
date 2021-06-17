package org.tikv.common.region;

import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb;

public class UnreachableStoreChecker implements Callable<Object> {
  private static final long MAX_CHECK_STORE_EXIST_TICK = 10;
  private ConcurrentHashMap<Long, TiStore> stores;
  private List<TiStore> taskQueue;
  private final ChannelFactory channelFactory;
  private final ReadOnlyPDClient pdClient;
  private long checkStoreExistTick;

  public UnreachableStoreChecker(ChannelFactory channelFactory, ReadOnlyPDClient pdClient) {
    this.stores = new ConcurrentHashMap();
    this.taskQueue = new LinkedList<>();
    this.channelFactory = channelFactory;
    this.pdClient = pdClient;
    this.checkStoreExistTick = 0;
  }

  public void scheduleStoreHealthCheck(TiStore store) {
    TiStore oldStore = this.stores.get(Long.valueOf(store.getId()));
    if (oldStore != null) {
      return;
    }
    synchronized (this.taskQueue) {
      this.stores.put(Long.valueOf(store.getId()), store);
      this.taskQueue.add(store);
    }
  }

  private List<TiStore> getUnhealthStore() {
    synchronized (this.taskQueue) {
      List<TiStore> unhealthStore = new LinkedList<>();
      unhealthStore.addAll(this.taskQueue);
      return unhealthStore;
    }
  }

  @Override
  public Object call() throws Exception {
    List<TiStore> unhealthStore = getUnhealthStore();
    List<TiStore> restStore = new LinkedList<>();
    checkStoreExistTick += 1;
    for (TiStore store : unhealthStore) {
      String addressStr = store.getStore().getAddress();
      ManagedChannel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
      HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(channel);
      HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
      try {
        HealthCheckResponse resp = stub.check(req);
        if (resp.getStatus() == HealthCheckResponse.ServingStatus.SERVING) {
          store.markReachable();
          this.stores.remove(Long.valueOf(store.getId()));
          continue;
        }
      } finally {
      }
      if (checkStoreExistTick > MAX_CHECK_STORE_EXIST_TICK) {
        try {
          Metapb.Store s = pdClient.getStore(ConcreteBackOffer.newGetBackOff(), store.getId());
          if (s.getState() == Metapb.StoreState.Offline || s.getState() == Metapb.StoreState.Tombstone) {
            continue;
          }
        } finally {
        }
      }
      restStore.add(store);
    }
    if (checkStoreExistTick > MAX_CHECK_STORE_EXIST_TICK) {
      checkStoreExistTick = 0;
    }
    synchronized (this.taskQueue) {
      int idx = unhealthStore.size();
      if (idx < this.taskQueue.size()) {
        for (int i = idx; i < this.taskQueue.size(); i++) {
          restStore.add(this.taskQueue.get(i));
        }
      }
      this.taskQueue = restStore;
    }
    return null;
  }
}
