package org.tikv.common.replica;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Iterator;
import java.util.List;
import org.tikv.kvproto.Metapb;

public class Region {
  private final Metapb.Region region;
  private final Store[] stores;
  private Store leaderStore;

  public Region(
      final Metapb.Region region,
      final Metapb.Peer leader,
      final List<Metapb.Peer> peers,
      final List<Metapb.Store> stores) {
    this.region = region;
    this.stores = new Store[stores.size()];
    Iterator<Metapb.Peer> peer = peers.iterator();
    Iterator<Metapb.Store> store = stores.iterator();
    for (int idx = 0; idx < peers.size(); idx++) {
      Metapb.Peer currentPeer = peer.next();
      boolean isLeader = currentPeer.equals(leader);
      this.stores[idx] = new Store(currentPeer, store.next(), isLeader);
      if (isLeader) {
        leaderStore = this.stores[idx];
      }
    }
  }

  public Store[] getStores() {
    return stores;
  }

  public Store getLeader() {
    return leaderStore;
  }

  public long getId() {
    return region.getId();
  }

  public byte[] getStartKey() {
    return region.getStartKey().toByteArray();
  }

  public byte[] getEndKey() {
    return region.getEndKey().toByteArray();
  }

  public String toString() {
    return toStringHelper(this).add("region", region).add("stores", stores).toString();
  }
}
