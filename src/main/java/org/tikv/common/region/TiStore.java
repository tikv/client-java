package org.tikv.common.region;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.tikv.kvproto.Metapb;

public class TiStore implements Serializable {
  private static long MAX_FAIL_FORWARD_TIMES = 4;
  private final Metapb.Store store;
  private final Metapb.Store proxyStore;
  private AtomicBoolean reachable;
  private AtomicBoolean valid;
  private AtomicLong failForwardCount;
  private AtomicBoolean canForward;

  public TiStore(Metapb.Store store) {
    this.store = store;
    this.reachable = new AtomicBoolean(true);
    this.valid = new AtomicBoolean(true);
    this.canForward = new AtomicBoolean(true);
    this.proxyStore = null;
    this.failForwardCount = new AtomicLong(0);
  }

  private TiStore(Metapb.Store store, Metapb.Store proxyStore) {
    this.store = store;
    if (proxyStore != null) {
      this.reachable = new AtomicBoolean(false);
    } else {
      this.reachable = new AtomicBoolean(true);
    }
    this.valid = new AtomicBoolean(true);
    this.canForward = new AtomicBoolean(true);
    this.proxyStore = proxyStore;
    this.failForwardCount = new AtomicLong(0);
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TiStore)) {
      return super.equals(obj);
    }
    TiStore other = (TiStore) obj;
    if (!this.store.equals(other.store)) {
      return false;
    }

    if (proxyStore == null && other.proxyStore == null) {
      return true;
    }
    if (proxyStore != null && other.proxyStore != null) {
      return proxyStore.equals(other.proxyStore);
    }
    return false;
  }

  public TiStore withProxy(Metapb.Store proxyStore) {
    return new TiStore(this.store, proxyStore);
  }

  public void markUnreachable() {
    this.reachable.set(false);
  }

  public void markReachable() {
    this.reachable.set(true);
  }

  public boolean isReachable() {
    return this.reachable.get();
  }

  public boolean isValid() {
    return this.valid.get();
  }

  public void markInvalid() {
    this.valid.set(false);
  }

  public void forwardFail() {
    if (this.canForward.get()) {
      if (this.failForwardCount.addAndGet(1) >= MAX_FAIL_FORWARD_TIMES) {
        this.canForward.set(false);
      }
    }
  }

  public void makrCanForward() {
    this.failForwardCount.set(0);
    this.canForward.set(true);
  }

  public boolean canForwardFirst() {
    return this.canForward.get();
  }

  public Metapb.Store getStore() {
    return this.store;
  }

  public String getAddress() {
    return this.store.getAddress();
  }

  public Metapb.Store getProxyStore() {
    return this.proxyStore;
  }

  public long getId() {
    return this.store.getId();
  }
}
