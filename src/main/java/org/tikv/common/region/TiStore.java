package org.tikv.common.region;

import java.util.concurrent.atomic.AtomicBoolean;
import org.tikv.kvproto.Metapb;

public class TiStore {
  private final Metapb.Store store;
  private final Metapb.Store proxyStore;
  private AtomicBoolean unreachable;

  public TiStore(Metapb.Store store) {
    this.store = store;
    this.unreachable = new AtomicBoolean(false);
    this.proxyStore = null;
  }

  private TiStore(Metapb.Store store, Metapb.Store proxyStore, boolean unreachable) {
    this.store = store;
    this.unreachable = new AtomicBoolean(unreachable);
    this.proxyStore = proxyStore;
  }

  public TiStore withProxy(Metapb.Store proxyStore) {
    return new TiStore(this.store, proxyStore, this.unreachable.get());
  }

  public void markUnreachable() {
    this.unreachable.set(true);
  }

  public void markReachable() {
    this.unreachable.set(false);
  }

  public boolean isUnreachable() {
    return this.unreachable.get();
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
