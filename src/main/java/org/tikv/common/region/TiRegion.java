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

package org.tikv.common.region;

import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.key.Key;
import org.tikv.common.replica.ReplicaSelector;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;

public class TiRegion implements Serializable {
  private static final Logger logger = LoggerFactory.getLogger(TiRegion.class);

  private final Region meta;
  private final IsolationLevel isolationLevel;
  private final Kvrpcpb.CommandPri commandPri;
  private final TiConfiguration conf;
  private final Peer leader;
  private final ReplicaSelector replicaSelector;
  private final List<Peer> replicaList;
  private int replicaIdx;
  private final List<Peer> peers;
  private final List<TiStore> stores;

  public TiRegion(
      TiConfiguration conf, Region meta, Peer leader, List<Peer> peers, List<TiStore> stores) {
    this.conf = Objects.requireNonNull(conf, "conf is null");
    this.meta = Objects.requireNonNull(meta, "meta is null");
    this.isolationLevel = conf.getIsolationLevel();
    this.commandPri = conf.getCommandPriority();
    this.peers = peers;
    this.stores = stores;
    this.replicaSelector = conf.getReplicaSelector();
    if (leader == null || leader.getId() == 0) {
      if (meta.getPeersCount() == 0) {
        throw new TiClientInternalException("Empty peer list for region " + meta.getId());
      }
      // region's first peer is leader.
      this.leader = meta.getPeers(0);
    } else {
      this.leader = leader;
    }

    // init replicaList
    replicaList =
        replicaSelector
            .select(new org.tikv.common.replica.Region(meta, this.leader, peers, stores))
            .stream()
            .map(org.tikv.common.replica.Store::getPeer)
            .collect(Collectors.toList());
    replicaIdx = 0;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public Peer getLeader() {
    return leader;
  }

  public List<Peer> getFollowerList() {
    List<Peer> peers = new ArrayList<>();
    for (Peer peer : getMeta().getPeersList()) {
      if (!peer.equals(this.leader)) {
        if (peer.getRole().equals(Metapb.PeerRole.Voter)) {
          peers.add(peer);
        }
      }
    }
    return peers;
  }

  public List<Peer> getLearnerList() {
    List<Peer> peers = new ArrayList<>();
    for (Peer peer : getMeta().getPeersList()) {
      if (peer.getRole().equals(Metapb.PeerRole.Learner)) {
        peers.add(peer);
      }
    }
    return peers;
  }

  public List<Peer> getPeersList() {
    return getMeta().getPeersList();
  }

  public Peer getCurrentReplica() {
    return replicaList.get(replicaIdx);
  }

  public Peer getNextReplica() {
    replicaIdx = (replicaIdx + 1) % replicaList.size();
    return getCurrentReplica();
  }

  private boolean isLeader(Peer peer) {
    return getLeader().equals(peer);
  }

  public long getId() {
    return this.meta.getId();
  }

  public ByteString getStartKey() {
    return meta.getStartKey();
  }

  public boolean contains(Key key) {
    return KeyRangeUtils.makeRange(this.getStartKey(), this.getEndKey()).contains(key);
  }

  public ByteString getEndKey() {
    return meta.getEndKey();
  }

  public Kvrpcpb.Context getLeaderContext() {
    return getContext(this.leader, java.util.Collections.emptySet(), false);
  }

  public Kvrpcpb.Context getReplicaContext(Set<Long> resolvedLocks, TiStoreType storeType) {
    Peer currentPeer = getCurrentReplica();
    boolean replicaRead = !isLeader(currentPeer) && TiStoreType.TiKV.equals(storeType);
    return getContext(currentPeer, resolvedLocks, replicaRead);
  }

  public Kvrpcpb.Context getReplicaContext(Peer currentPeer, Set<Long> resolvedLocks) {
    return getContext(currentPeer, resolvedLocks, false);
  }

  public Kvrpcpb.Context getReplicaContext(Peer currentPeer) {
    return getContext(currentPeer, java.util.Collections.emptySet(), false);
  }

  private Kvrpcpb.Context getContext(
      Peer currentPeer, Set<Long> resolvedLocks, boolean replicaRead) {

    Kvrpcpb.Context.Builder builder = Kvrpcpb.Context.newBuilder();
    builder
        .setIsolationLevel(this.isolationLevel)
        .setPriority(this.commandPri)
        .setRegionId(meta.getId())
        .setPeer(currentPeer)
        .setReplicaRead(replicaRead)
        .setRegionEpoch(this.meta.getRegionEpoch());
    builder.addAllResolvedLocks(resolvedLocks);
    return builder.build();
  }

  // getVerID returns the Region's RegionVerID.
  public RegionVerID getVerID() {
    return new RegionVerID(
        meta.getId(), meta.getRegionEpoch().getConfVer(), meta.getRegionEpoch().getVersion());
  }

  /**
   * switches current peer to the one on specific store. It return false if no peer matches the
   * storeID.
   *
   * @param leaderStoreID is leader peer id.
   * @return null if no peers matches the store id.
   */
  public TiRegion switchPeer(long leaderStoreID) {
    List<Peer> peers = meta.getPeersList();
    for (Peer p : peers) {
      if (p.getStoreId() == leaderStoreID) {
        return new TiRegion(this.conf, this.meta, p, peers, this.stores);
      }
    }
    return null;
  }

  public boolean isMoreThan(ByteString key) {
    return FastByteComparisons.compareTo(
            meta.getStartKey().toByteArray(),
            0,
            meta.getStartKey().size(),
            key.toByteArray(),
            0,
            key.size())
        > 0;
  }

  public boolean isLessThan(ByteString key) {
    return FastByteComparisons.compareTo(
            meta.getEndKey().toByteArray(),
            0,
            meta.getEndKey().size(),
            key.toByteArray(),
            0,
            key.size())
        <= 0;
  }

  public boolean contains(ByteString key) {
    return !isMoreThan(key) && !isLessThan(key);
  }

  public boolean isValid() {
    return leader != null && meta != null;
  }

  public Metapb.RegionEpoch getRegionEpoch() {
    return this.meta.getRegionEpoch();
  }

  public Region getMeta() {
    return meta;
  }

  @Override
  public boolean equals(final Object another) {
    if (!(another instanceof TiRegion)) {
      return false;
    }
    TiRegion anotherRegion = ((TiRegion) another);
    return anotherRegion.meta.equals(this.meta)
        && anotherRegion.leader.equals(this.leader)
        && anotherRegion.commandPri.equals(this.commandPri)
        && anotherRegion.isolationLevel.equals(this.isolationLevel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, leader, isolationLevel, commandPri);
  }

  @Override
  public String toString() {
    return String.format(
        "{Region[%d] ConfVer[%d] Version[%d] Store[%d] KeyRange[%s]:[%s]}",
        getId(),
        getRegionEpoch().getConfVer(),
        getRegionEpoch().getVersion(),
        getLeader().getStoreId(),
        KeyUtils.formatBytesUTF8(getStartKey()),
        KeyUtils.formatBytesUTF8(getEndKey()));
  }

  public class RegionVerID {
    final long id;
    final long confVer;
    final long ver;

    RegionVerID(long id, long confVer, long ver) {
      this.id = id;
      this.confVer = confVer;
      this.ver = ver;
    }

    public long getId() {
      return id;
    }

    public long getConfVer() {
      return confVer;
    }

    public long getVer() {
      return ver;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof RegionVerID)) {
        return false;
      }

      RegionVerID that = (RegionVerID) other;
      return id == that.id && confVer == that.confVer && ver == that.ver;
    }

    @Override
    public int hashCode() {
      int hash = Long.hashCode(id);
      hash = hash * 31 + Long.hashCode(confVer);
      hash = hash * 31 + Long.hashCode(ver);
      return hash;
    }
  }
}
