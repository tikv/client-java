package org.tikv.common.region;

import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.util.LinkedList;
import java.util.List;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb;

public class UnreachableStoreChecker implements Runnable {
  private ConcurrentHashMap<Long, TiStore> stores;
  private BlockingQueue<TiStore> taskQueue;
  private final ChannelFactory channelFactory;
  private final ReadOnlyPDClient pdClient;

  public UnreachableStoreChecker(ChannelFactory channelFactory, ReadOnlyPDClient pdClient) {
    this.stores = new ConcurrentHashMap();
    this.taskQueue = new LinkedBlockingQueue<>();
    this.channelFactory = channelFactory;
    this.pdClient = pdClient;
  }

  public void scheduleStoreHealthCheck(TiStore store) {
    TiStore oldStore = this.stores.get(Long.valueOf(store.getId()));
    if (oldStore == store) {
      return;
    }
    this.stores.put(Long.valueOf(store.getId()), store);
    if (!this.taskQueue.add(store)) {
      // add queue false, mark it reachable so that it can be put again.
      store.markReachable();
    }
  }

  private List<TiStore> getUnhealthStore() {
    List<TiStore> unhealthStore = new LinkedList<>();
    while (!this.taskQueue.isEmpty()) {
      try {
        TiStore store = this.taskQueue.take();
        unhealthStore.add(store);
      } catch (Exception e) {
        return unhealthStore;
      }
    }
    return unhealthStore;
  }

  @Override
  public void run() {
    List<TiStore> unhealthStore = getUnhealthStore();
    for (TiStore store : unhealthStore) {
      if (!store.isUnreachable()) {
        continue;
      }
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
        Metapb.Store newStore = pdClient.getStore(ConcreteBackOffer.newRawKVBackOff(), store.getId());
        if (newStore.getState() == Metapb.StoreState.Tombstone) {
            continue;
        }
        this.taskQueue.add(store);
      } catch (Exception e) {
        this.taskQueue.add(store);
      }
    }
  }
}
