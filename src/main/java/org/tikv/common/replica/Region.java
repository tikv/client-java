/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.tikv.common.replica;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Iterator;
import java.util.List;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;

public class Region {
  private final Metapb.Region region;
  private final Store[] stores;
  private Store leaderStore;

  public Region(
      final Metapb.Region region,
      final Metapb.Peer leader,
      final List<Metapb.Peer> peers,
      final List<TiStore> stores) {
    this.region = region;
    this.stores = new Store[stores.size()];
    Iterator<Metapb.Peer> peer = peers.iterator();
    Iterator<TiStore> store = stores.iterator();
    for (int idx = 0; idx < peers.size(); idx++) {
      Metapb.Peer currentPeer = peer.next();
      boolean isLeader = currentPeer.equals(leader);
      this.stores[idx] = new Store(currentPeer, store.next().getStore(), isLeader);
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
