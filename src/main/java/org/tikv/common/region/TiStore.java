package org.tikv.common.region;

import java.util.concurrent.atomic.AtomicBoolean;
import org.tikv.kvproto.Metapb;

public class TiStore {
  private final Metapb.Store store;
  private AtomicBoolean unreachable;

  public TiStore(Metapb.Store store) {
    this.store = store;
    this.unreachable.set(false);
  }

  public void invalid() {
    this.unreachable.set(true);
  }

  public boolean isUnreachable() {
    return this.unreachable.get();
  }

  public Metapb.Store getStore() {
    return this.store;
  }
}
