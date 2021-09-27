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
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.HostMapping;
import org.tikv.common.pd.PDUtils;

public class ChannelFactory implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ChannelFactory.class);

  private final int maxFrameSize;
  private final int keepaliveTime;
  private final int keepaliveTimeout;
  private final ConcurrentHashMap<String, ManagedChannel> connPool = new ConcurrentHashMap<>();
  private final SslContextBuilder sslContextBuilder;

  public ChannelFactory(int maxFrameSize, int keepaliveTime, int keepaliveTimeout) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.sslContextBuilder = null;
  }

  public ChannelFactory(
      int maxFrameSize,
      int keepaliveTime,
      int keepaliveTimeout,
      String trustCertCollectionFilePath,
      String keyCertChainFilePath,
      String keyFilePath) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.sslContextBuilder =
        getSslContextBuilder(trustCertCollectionFilePath, keyCertChainFilePath, keyFilePath);
  }

  private SslContextBuilder getSslContextBuilder(
      String trustCertCollectionFilePath, String keyCertChainFilePath, String keyFilePath) {
    SslContextBuilder builder = GrpcSslContexts.forClient();
    if (trustCertCollectionFilePath != null) {
      builder.trustManager(new File(trustCertCollectionFilePath));
    }
    if (keyCertChainFilePath != null && keyFilePath != null) {
      builder.keyManager(new File(keyCertChainFilePath), new File(keyFilePath));
    }
    return builder;
  }

  public ManagedChannel getChannel(String addressStr, HostMapping hostMapping) {
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
            mappedAddr = hostMapping.getMappedURI(address);
          } catch (Exception e) {
            throw new IllegalArgumentException("failed to get mapped address " + address, e);
          }

          // Channel should be lazy without actual connection until first call
          // So a coarse grain lock is ok here
          NettyChannelBuilder builder =
              NettyChannelBuilder.forAddress(mappedAddr.getHost(), mappedAddr.getPort())
                  .maxInboundMessageSize(maxFrameSize)
                  .keepAliveTime(keepaliveTime, TimeUnit.SECONDS)
                  .keepAliveTimeout(keepaliveTimeout, TimeUnit.SECONDS)
                  .keepAliveWithoutCalls(true)
                  .idleTimeout(60, TimeUnit.SECONDS);

          if (sslContextBuilder == null) {
            return builder.usePlaintext(true).build();
          } else {
            SslContext sslContext = null;
            try {
              sslContext = sslContextBuilder.build();
            } catch (SSLException e) {
              logger.error("create ssl context failed!", e);
              return null;
            }
            return builder.sslContext(sslContext).build();
          }
        });
  }

  public void close() {
    for (ManagedChannel ch : connPool.values()) {
      ch.shutdown();
    }
    connPool.clear();
  }
}
