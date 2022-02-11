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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.tikv.kvproto.Metapb;

public class TiStore implements Serializable {
  private final Metapb.Store store;
  private final Metapb.Store proxyStore;
  private final AtomicBoolean reachable;
  private final AtomicBoolean valid;

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
