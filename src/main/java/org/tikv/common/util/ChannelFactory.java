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

package org.tikv.common.util;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.pd.PDUtils;

public class ChannelFactory implements AutoCloseable {
  private final int maxFrameSize;
  private final ConcurrentHashMap<String, ManagedChannel> connPool = new ConcurrentHashMap<>();

  public ChannelFactory(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
  }

  public ManagedChannel getChannel(String addressStr) {
    return connPool.computeIfAbsent(
        addressStr,
        key -> {
          URI address;
          try {
            address = PDUtils.addrToUri(key);
          } catch (Exception e) {
            throw new IllegalArgumentException("failed to form address " + key, e);
          }
          // Channel should be lazy without actual connection until first call
          // So a coarse grain lock is ok here
          return ManagedChannelBuilder.forAddress(address.getHost(), address.getPort())
              .maxInboundMessageSize(maxFrameSize)
              .usePlaintext(true)
              .idleTimeout(60, TimeUnit.SECONDS)
              .build();
        });
  }

  public ManagedChannel getChannel(String addressStr, ReadOnlyPDClient client) {
    return connPool.computeIfAbsent(
        addressStr,
        key -> {
          URI address;
          URI mappedAddr;
          try {
            address = PDUtils.addrToUri(key);
          } catch (Exception e) {
            throw new IllegalArgumentException("failed to form address " + key, e);
          }
          try {
            mappedAddr = client.getMappedURI(address);
          } catch (Exception e) {
            throw new IllegalArgumentException("failed to get mapped address " + address, e);
          }
          // Channel should be lazy without actual connection until first call
          // So a coarse grain lock is ok here
          return ManagedChannelBuilder.forAddress(mappedAddr.getHost(), mappedAddr.getPort())
              .maxInboundMessageSize(maxFrameSize)
              .usePlaintext(true)
              .idleTimeout(60, TimeUnit.SECONDS)
              .build();
        });
  }

  public void close() {
    for (ManagedChannel ch : connPool.values()) {
      ch.shutdown();
    }
    connPool.clear();
  }
}
