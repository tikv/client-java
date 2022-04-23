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

package org.tikv.common.util;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.HostMapping;
import org.tikv.common.pd.PDUtils;

public class ChannelFactory implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ChannelFactory.class);

  private final int maxFrameSize;
  private final int keepaliveTime;
  private final int keepaliveTimeout;
  private final int idleTimeout;
  private final ConcurrentHashMap<String, ManagedChannel> connPool = new ConcurrentHashMap<>();
  private final CertContext certContext;
  private static final String PUB_KEY_INFRA = "PKIX";

  @VisibleForTesting
  public abstract static class CertContext {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private SslContextBuilder sslContextBuilder;

    protected abstract boolean isModified();

    protected abstract SslContextBuilder createSslContextBuilder();

    public void reload(Map<?, ?> connPool) {
      try {
        lock.writeLock().lock();
        if (isModified()) {
          logger.info("reload ssl context");
          sslContextBuilder = createSslContextBuilder();
          if (connPool != null) {
            logger.info("invalidate connection pool");
            connPool.clear();
          }
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    public SslContextBuilder getSslContextBuilder() {
      try {
        lock.readLock().lock();
        return sslContextBuilder;
      } finally {
        lock.readLock().unlock();
      }
    }
  }

  @VisibleForTesting
  public static class JksContext extends CertContext {
    private long keyLastModified;
    private long trustLastModified;

    private final String keyPath;
    private final String keyPassword;
    private final String trustPath;
    private final String trustPassword;

    public JksContext(String keyPath, String keyPassword, String trustPath, String trustPassword) {
      this.keyLastModified = 0;
      this.trustLastModified = 0;

      this.keyPath = keyPath;
      this.keyPassword = keyPassword;
      this.trustPath = trustPath;
      this.trustPassword = trustPassword;
    }

    @Override
    protected boolean isModified() {
      long a = new File(keyPath).lastModified();
      long b = new File(trustPath).lastModified();

      boolean changed = this.keyLastModified != a || this.trustLastModified != b;

      if (changed) {
        this.keyLastModified = a;
        this.trustLastModified = b;
      }

      return changed;
    }

    @Override
    protected SslContextBuilder createSslContextBuilder() {
      SslContextBuilder builder = GrpcSslContexts.forClient();
      try {
        if (keyPath != null && keyPassword != null) {
          KeyStore keyStore = KeyStore.getInstance("JKS");
          keyStore.load(new FileInputStream(keyPath), keyPassword.toCharArray());
          KeyManagerFactory keyManagerFactory =
              KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          keyManagerFactory.init(keyStore, keyPassword.toCharArray());
          builder.keyManager(keyManagerFactory);
        }
        if (trustPath != null && trustPassword != null) {
          KeyStore trustStore = KeyStore.getInstance("JKS");
          trustStore.load(new FileInputStream(trustPath), trustPassword.toCharArray());
          TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(PUB_KEY_INFRA);
          trustManagerFactory.init(trustStore);
          builder.trustManager(trustManagerFactory);
        }
      } catch (Exception e) {
        logger.error("JKS SSL context builder failed!", e);
        throw new RuntimeException(e);
      }
      return builder;
    }
  }


  @VisibleForTesting
  public static class OpenSslContext extends CertContext {
    private long trustLastModified;
    private long chainLastModified;
    private long keyLastModified;

    private final String trustPath;
    private final String chainPath;
    private final String keyPath;

    public OpenSslContext(String trustPath, String chainPath, String keyPath) {
      this.trustLastModified = 0;
      this.chainLastModified = 0;
      this.keyLastModified = 0;

      this.trustPath = trustPath;
      this.chainPath = chainPath;
      this.keyPath = keyPath;
    }

    @Override
    protected boolean isModified() {
      long a = new File(trustPath).lastModified();
      long b = new File(chainPath).lastModified();
      long c = new File(keyPath).lastModified();

      boolean changed =
          this.trustLastModified != a || this.chainLastModified != b || this.keyLastModified != c;

      if (changed) {
        this.trustLastModified = a;
        this.chainLastModified = b;
        this.keyLastModified = c;
      }

      return changed;
    }

    @Override
    protected SslContextBuilder createSslContextBuilder() {
      SslContextBuilder builder = GrpcSslContexts.forClient();
      if (trustPath != null) {
        builder.trustManager(new File(trustPath));
      }
      if (chainPath != null && keyPath != null) {
        builder.keyManager(new File(chainPath), new File(keyPath));
      }
      return builder;
    }
  }

  public ChannelFactory(
      int maxFrameSize, int keepaliveTime, int keepaliveTimeout, int idleTimeout) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.idleTimeout = idleTimeout;
    this.certContext = null;
  }

  public ChannelFactory(
      int maxFrameSize,
      int keepaliveTime,
      int keepaliveTimeout,
      int idleTimeout,
      String trustCertCollectionFilePath,
      String keyCertChainFilePath,
      String keyFilePath) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.idleTimeout = idleTimeout;
    this.certContext =
        new OpenSslContext(trustCertCollectionFilePath, keyCertChainFilePath, keyFilePath);
  }

  public ChannelFactory(
      int maxFrameSize,
      int keepaliveTime,
      int keepaliveTimeout,
      int idleTimeout,
      String jksKeyPath,
      String jksKeyPassword,
      String jksTrustPath,
      String jksTrustPassword) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.idleTimeout = idleTimeout;
    this.certContext = new JksContext(jksKeyPath, jksKeyPassword, jksTrustPath, jksTrustPassword);
  }

  public ManagedChannel getChannel(String addressStr, HostMapping hostMapping) {
    SslContextBuilder sslContextBuilder = null;
    if (certContext != null) {
      certContext.reload(connPool);
      sslContextBuilder = certContext.getSslContextBuilder();
    }

    SslContextBuilder finalSslContextBuilder = sslContextBuilder;
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
                  .idleTimeout(idleTimeout, TimeUnit.SECONDS);

          if (certContext == null) {
            return builder.usePlaintext().build();
          } else {
            SslContext sslContext;
            try {
              sslContext = finalSslContextBuilder.build();
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
