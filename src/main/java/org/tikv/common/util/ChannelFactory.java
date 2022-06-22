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
import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
  private static final String PUB_KEY_INFRA = "PKIX";

  // After `connRecycleTime` seconds elapses, the old channels will be forced to shut down,
  // to avoid using the old context all the time including potential channel leak.
  private final long connRecycleTime;
  private final int maxFrameSize;
  private final int keepaliveTime;
  private final int keepaliveTimeout;
  private final int idleTimeout;
  private final CertContext certContext;
  private final CertWatcher certWatcher;

  @VisibleForTesting
  public final ConcurrentHashMap<String, ManagedChannel> connPool = new ConcurrentHashMap<>();

  private final AtomicReference<SslContextBuilder> sslContextBuilder = new AtomicReference<>();

  private final ScheduledExecutorService recycler;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  @VisibleForTesting
  public static class CertWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CertWatcher.class);
    private final List<File> targets;
    private final List<Long> lastReload = new ArrayList<>();
    private final ScheduledExecutorService executorService =
        Executors.newSingleThreadScheduledExecutor();
    private final Runnable onChange;

    public CertWatcher(long pollInterval, List<File> targets, Runnable onChange) {
      this.targets = targets;
      this.onChange = onChange;

      for (File ignored : targets) {
        lastReload.add(0L);
      }

      executorService.scheduleAtFixedRate(
          this::tryReload, pollInterval, pollInterval, TimeUnit.SECONDS);
    }

    // If any execution of the task encounters an exception, subsequent executions are suppressed.
    private void tryReload() {
      // Add exception handling to avoid schedule stop.
      try {
        if (needReload()) {
          onChange.run();
        }
      } catch (Exception e) {
        logger.error("Failed to reload cert!", e);
      }
    }

    private boolean needReload() {
      boolean needReload = false;
      // Check all the modification of the `targets`.
      // If one of them changed, means to need reload.
      for (int i = 0; i < targets.size(); i++) {
        try {
          long lastModified = targets.get(i).lastModified();
          if (lastModified != lastReload.get(i)) {
            lastReload.set(i, lastModified);
            logger.warn("detected ssl context changes: {}", targets.get(i));
            needReload = true;
          }
        } catch (Exception e) {
          logger.error("fail to check the status of ssl context files", e);
        }
      }
      return needReload;
    }

    @Override
    public void close() {
      executorService.shutdown();
    }
  }

  @VisibleForTesting
  public abstract static class CertContext {
    public abstract SslContextBuilder createSslContextBuilder();
  }

  public static class JksContext extends CertContext {
    private final String keyPath;
    private final String keyPassword;
    private final String trustPath;
    private final String trustPassword;

    public JksContext(String keyPath, String keyPassword, String trustPath, String trustPassword) {
      this.keyPath = keyPath;
      this.keyPassword = keyPassword;
      this.trustPath = trustPath;
      this.trustPassword = trustPassword;
    }

    @Override
    public SslContextBuilder createSslContextBuilder() {
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
        throw new IllegalArgumentException(e);
      }
      return builder;
    }
  }

  @VisibleForTesting
  public static class OpenSslContext extends CertContext {
    private final String trustPath;
    private final String chainPath;
    private final String keyPath;

    public OpenSslContext(String trustPath, String chainPath, String keyPath) {
      this.trustPath = trustPath;
      this.chainPath = chainPath;
      this.keyPath = keyPath;
    }

    @Override
    public SslContextBuilder createSslContextBuilder() {
      SslContextBuilder builder = GrpcSslContexts.forClient();
      try {
        if (trustPath != null) {
          builder.trustManager(new File(trustPath));
        }
        if (chainPath != null && keyPath != null) {
          builder.keyManager(new File(chainPath), new File(keyPath));
        }
      } catch (Exception e) {
        logger.error("Failed to create ssl context builder", e);
        throw new IllegalArgumentException(e);
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
    this.certWatcher = null;
    this.certContext = null;
    this.recycler = null;
    this.connRecycleTime = 0;
  }

  public ChannelFactory(
      int maxFrameSize,
      int keepaliveTime,
      int keepaliveTimeout,
      int idleTimeout,
      long connRecycleTime,
      long certReloadInterval,
      String trustCertCollectionFilePath,
      String keyCertChainFilePath,
      String keyFilePath) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.idleTimeout = idleTimeout;
    this.connRecycleTime = connRecycleTime;
    this.certContext =
        new OpenSslContext(trustCertCollectionFilePath, keyCertChainFilePath, keyFilePath);
    this.recycler = Executors.newSingleThreadScheduledExecutor();

    File trustCert = new File(trustCertCollectionFilePath);
    File keyCert = new File(keyCertChainFilePath);
    File key = new File(keyFilePath);

    if (certReloadInterval > 0) {
      onCertChange();
      this.certWatcher =
          new CertWatcher(
              certReloadInterval, ImmutableList.of(trustCert, keyCert, key), this::onCertChange);
    } else {
      this.certWatcher = null;
    }
  }

  public ChannelFactory(
      int maxFrameSize,
      int keepaliveTime,
      int keepaliveTimeout,
      int idleTimeout,
      long connRecycleTime,
      long certReloadInterval,
      String jksKeyPath,
      String jksKeyPassword,
      String jksTrustPath,
      String jksTrustPassword) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.idleTimeout = idleTimeout;
    this.connRecycleTime = connRecycleTime;
    this.certContext = new JksContext(jksKeyPath, jksKeyPassword, jksTrustPath, jksTrustPassword);
    this.recycler = Executors.newSingleThreadScheduledExecutor();

    File jksKey = new File(jksKeyPath);
    File jksTrust = new File(jksTrustPath);
    if (certReloadInterval > 0) {
      onCertChange();
      this.certWatcher =
          new CertWatcher(
              certReloadInterval, ImmutableList.of(jksKey, jksTrust), this::onCertChange);
    } else {
      this.certWatcher = null;
    }
  }

  private void onCertChange() {
    try {
      SslContextBuilder newBuilder = certContext.createSslContextBuilder();
      lock.writeLock().lock();
      sslContextBuilder.set(newBuilder);

      List<ManagedChannel> pending = new ArrayList<>(connPool.values());
      recycler.schedule(() -> cleanExpiredConn(pending), connRecycleTime, TimeUnit.SECONDS);

      connPool.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ManagedChannel getChannel(String address, HostMapping mapping) {
    if (certContext != null) {
      try {
        lock.readLock().lock();
        return connPool.computeIfAbsent(
            address, key -> createChannel(sslContextBuilder.get(), address, mapping));
      } finally {
        lock.readLock().unlock();
      }
    }
    return connPool.computeIfAbsent(address, key -> createChannel(null, address, mapping));
  }

  private ManagedChannel createChannel(
      SslContextBuilder sslContextBuilder, String address, HostMapping mapping) {
    URI uri, mapped;
    try {
      uri = PDUtils.addrToUri(address);
    } catch (Exception e) {
      throw new IllegalArgumentException("failed to form address " + address, e);
    }
    try {
      mapped = mapping.getMappedURI(uri);
    } catch (Exception e) {
      throw new IllegalArgumentException("failed to get mapped address " + uri, e);
    }

    // Channel should be lazy without actual connection until first call
    // So a coarse grain lock is ok here
    NettyChannelBuilder builder =
        NettyChannelBuilder.forAddress(mapped.getHost(), mapped.getPort())
            .maxInboundMessageSize(maxFrameSize)
            .keepAliveTime(keepaliveTime, TimeUnit.SECONDS)
            .keepAliveTimeout(keepaliveTimeout, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .idleTimeout(idleTimeout, TimeUnit.SECONDS);

    if (sslContextBuilder == null) {
      return builder.usePlaintext().build();
    } else {
      SslContext sslContext;
      try {
        sslContext = sslContextBuilder.build();
      } catch (SSLException e) {
        logger.error("create ssl context failed!", e);
        throw new IllegalArgumentException(e);
      }
      return builder.sslContext(sslContext).build();
    }
  }

  private void cleanExpiredConn(List<ManagedChannel> pending) {
    for (ManagedChannel channel : pending) {
      logger.info("cleaning expire channels");
      channel.shutdownNow();
      while (!channel.isShutdown()) {
        try {
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
          logger.warn("recycle channels timeout:", e);
        }
      }
    }
  }

  public void close() {
    for (ManagedChannel ch : connPool.values()) {
      ch.shutdown();
    }
    connPool.clear();

    if (recycler != null) {
      recycler.shutdown();
    }

    if (certWatcher != null) {
      certWatcher.close();
    }
  }
}
