/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv;

import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.tikv.event.CacheInvalidateEvent;
import org.tikv.region.RegionManager;

public class TiSession implements AutoCloseable {
  private static final Map<String, ManagedChannel> connPool = new HashMap<>();
  private final TiConfiguration conf;
  private Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;
  // below object creation is either heavy or making connection (pd), pending for lazy loading
  private volatile RegionManager regionManager;
  private volatile PDClient client;

  public TiSession(TiConfiguration conf) {
    this.conf = conf;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public PDClient getPDClient() {
    PDClient res = client;
    if (res == null) {
      synchronized (this) {
        if (client == null) {
          client = PDClient.createRaw(this);
        }
        res = client;
      }
    }
    return res;
  }

  public synchronized RegionManager getRegionManager() {
    RegionManager res = regionManager;
    if (res == null) {
      synchronized (this) {
        if (regionManager == null) {
          regionManager = new RegionManager(getPDClient());
        }
        res = regionManager;
      }
    }
    return res;
  }

  public synchronized ManagedChannel getChannel(String addressStr) {
    ManagedChannel channel = connPool.get(addressStr);
    if (channel == null) {
      HostAndPort address;
      try {
        address = HostAndPort.fromString(addressStr);
      } catch (Exception e) {
        throw new IllegalArgumentException("failed to form address");
      }

      // Channel should be lazy without actual connection until first call
      // So a coarse grain lock is ok here
      channel =
          ManagedChannelBuilder.forAddress(address.getHostText(), address.getPort())
              .maxInboundMessageSize(conf.getMaxFrameSize())
              .usePlaintext(true)
              .idleTimeout(60, TimeUnit.SECONDS)
              .build();
      connPool.put(addressStr, channel);
    }
    return channel;
  }

  public static TiSession create(TiConfiguration conf) {
    return new TiSession(conf);
  }

  public Function<CacheInvalidateEvent, Void> getCacheInvalidateCallback() {
    return cacheInvalidateCallback;
  }

  @Override
  public void close() throws Exception {}
}
