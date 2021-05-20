package org.tikv.common.replica;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.List;
import org.tikv.kvproto.Metapb;

public class Store {
  public static class Label {
    private final org.tikv.kvproto.Metapb.StoreLabel label;

    Label(org.tikv.kvproto.Metapb.StoreLabel label) {
      this.label = label;
    }

    public String getKey() {
      return label.getKey();
    }

    public String getValue() {
      return label.getValue();
    }
  }

  public enum State {
    Unknown,
    Up,
    Offline,
    Tombstone
  }

  private static final Label[] EMPTY_LABELS = new Label[0];
  private Label[] labels;
  private final Metapb.Peer peer;
  private final Metapb.Store store;
  private final boolean isLeader;

  Store(
      final org.tikv.kvproto.Metapb.Peer peer,
      final org.tikv.kvproto.Metapb.Store store,
      boolean isLeader) {
    this.peer = peer;
    this.store = store;
    this.isLeader = isLeader;
  }

  public Metapb.Peer getPeer() {
    return peer;
  }

  public Label[] getLabels() {
    if (labels == null) {
      List<Metapb.StoreLabel> labelList = store.getLabelsList();
      if (labelList.isEmpty()) {
        labels = EMPTY_LABELS;
      } else {
        labels = labelList.stream().map(Label::new).toArray(Label[]::new);
      }
    }
    return labels;
  }

  public boolean isLearner() {
    return peer.getRole() == Metapb.PeerRole.Learner;
  }

  public boolean isLeader() {
    return isLeader;
  }

  public boolean isFollower() {
    return peer.getRole() == Metapb.PeerRole.Voter && !isLeader;
  }

  public long getId() {
    return store.getId();
  }

  public String getAddress() {
    return store.getAddress();
  }

  public String getVersion() {
    return store.getVersion();
  }

  public State getState() {
    switch (store.getState()) {
      case Up:
        return State.Up;
      case Offline:
        return State.Offline;
      case Tombstone:
        return State.Tombstone;
      default:
        return State.Unknown;
    }
  }

  public boolean equals(Object o) {
    if (!(o instanceof Store)) {
      return false;
    }
    Store other = (Store) o;
    return this.peer.equals(other.peer);
  }

  public String toString() {
    return toStringHelper(this).add("peer", peer).add("store", store).toString();
  }
}
