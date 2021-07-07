package org.tikv.common.region;

import java.util.concurrent.atomic.AtomicBoolean;
import org.tikv.kvproto.Metapb;

public class TiStore {
  private final Metapb.Store store;
  private final Metapb.Store proxyStore;
  private AtomicBoolean reachable;
  private AtomicBoolean valid;

  public TiStore(Metapb.Store store) {
    this.store = store;
    this.reachable = new AtomicBoolean(true);
    this.valid = new AtomicBoolean(true);
    this.proxyStore = null;
  }

  private TiStore(Metapb.Store store, Metapb.Store proxyStore) {
    this.store = store;
    if (proxyStore != null) {
      this.reachable = new AtomicBoolean(false);
    } else {
      this.reachable = new AtomicBoolean(true);
    }
    this.valid = new AtomicBoolean(true);
    this.proxyStore = proxyStore;
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
