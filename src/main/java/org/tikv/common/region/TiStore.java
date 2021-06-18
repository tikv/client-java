package org.tikv.common.region;

import java.util.concurrent.atomic.AtomicBoolean;
import org.tikv.kvproto.Metapb;

public class TiStore {
  private final Metapb.Store store;
  private AtomicBoolean unreachable;

  public TiStore(Metapb.Store store) {
    this.store = store;
    this.unreachable = new AtomicBoolean(false);
  }

  public boolean markUnreachable() {
    return this.unreachable.compareAndSet(false, true);
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

  public long getId() {
    return this.store.getId();
  }
}
