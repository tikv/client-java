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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.HostMapping;
import org.tikv.common.pd.PDUtils;

public class ChannelFactory implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ChannelFactory.class);
  private static final String PUB_KEY_INFRA = "PKIX";

  private int connRecycleTime;
  private final int maxFrameSize;
  private final int keepaliveTime;
  private final int keepaliveTimeout;
  private final int idleTimeout;
  private final CertContext certContext;

  private int epoch = 0;

  @VisibleForTesting
  public final ConcurrentHashMap<Pair<Epoch<SslContextBuilder>, String>, ManagedChannel> connPool =
      new ConcurrentHashMap<>();

  private final AtomicReference<Epoch<SslContextBuilder>> sslContextBuilder =
      new AtomicReference<>(new Epoch<>(null, epoch));

  private final ScheduledExecutorService recycler = Executors.newScheduledThreadPool(1);

  private static class Epoch<T> {
    private final T inner;
    private final int epoch;

    public Epoch(T inner, int epoch) {
      this.inner = inner;
      this.epoch = epoch;
    }

    public T get() {
      return inner;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Epoch) {
        Epoch<?> o = (Epoch<?>) other;
        return o.epoch == epoch;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return epoch;
    }
  }

  @VisibleForTesting
  public static class CertWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CertWatcher.class);
    private final String path;
    private final ArrayList<String> targets = new ArrayList<>();
    private final BlockingDeque<Boolean> notify = new LinkedBlockingDeque<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    public CertWatcher(String path) {
      if (path == null) {
        throw new IllegalArgumentException("the base path of certs is missing");
      }
      this.path = path;
      notify.push(true);
      start();
    }

    private void watchLoop() {
      try {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Path p = Paths.get(path);
        p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

        while (true) {
          WatchKey key = watchService.take();
          boolean changed = false;
          OUTER:
          for (WatchEvent<?> event : key.pollEvents()) {
            for (String target : targets) {
              logger.info("detected file change: {}", event.context());
              if (event.context().toString().equals(target)) {
                changed = true;
                break OUTER;
              }
            }
          }
          if (changed) {
            notify.offer(true);
          }
          if (!key.reset()) {
            String msg = "fail to reset watch key";
            logger.warn(msg);
            throw new RuntimeException(msg);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void start() {
      executorService.submit(this::watchLoop);
    }

    public CertWatcher addTarget(String target) {
      targets.add(target);
      return this;
    }

    public boolean isModified() throws Exception {
      boolean modified = !notify.isEmpty();
      if (modified) {
        notify.take();
      }

      return modified;
    }

    @Override
    public void close() {
      executorService.shutdownNow();
    }
  }

  @VisibleForTesting
  public abstract static class CertContext implements AutoCloseable {
    public CertWatcher certWatcher;

    public abstract boolean isModified() throws Exception;

    public abstract SslContextBuilder createSslContextBuilder();

    @Override
    public void close() {
      certWatcher.close();
    }
  }

  public static class JksContext extends CertContext {
    private final String keyPath;
    private final String keyPassword;
    private final String trustPath;
    private final String trustPassword;

    public JksContext(
        String base, String keyPath, String keyPassword, String trustPath, String trustPassword) {
      this.keyPath = base + keyPath;
      this.keyPassword = keyPassword;
      this.trustPath = base + trustPath;
      this.trustPassword = trustPassword;
      certWatcher = new CertWatcher(base).addTarget(keyPath).addTarget(trustPath);
    }

    @Override
    public boolean isModified() throws Exception {
      return certWatcher.isModified();
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

    public OpenSslContext(String base, String trustPath, String chainPath, String keyPath) {
      this.trustPath = base + trustPath;
      this.chainPath = base + chainPath;
      this.keyPath = base + keyPath;

      certWatcher =
          new CertWatcher(base).addTarget(trustPath).addTarget(chainPath).addTarget(keyPath);
    }

    @Override
    public boolean isModified() throws Exception {
      return certWatcher.isModified();
    }

    @Override
    public SslContextBuilder createSslContextBuilder() {
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
      int connRecycleTime,
      String path,
      String trustCertCollectionFilePath,
      String keyCertChainFilePath,
      String keyFilePath) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.idleTimeout = idleTimeout;
    this.connRecycleTime = connRecycleTime;
    this.certContext =
        new OpenSslContext(path, trustCertCollectionFilePath, keyCertChainFilePath, keyFilePath);
  }

  public ChannelFactory(
      int maxFrameSize,
      int keepaliveTime,
      int keepaliveTimeout,
      int idleTimeout,
      int connRecycleTime,
      String path,
      String jksKeyPath,
      String jksKeyPassword,
      String jksTrustPath,
      String jksTrustPassword) {
    this.maxFrameSize = maxFrameSize;
    this.keepaliveTime = keepaliveTime;
    this.keepaliveTimeout = keepaliveTimeout;
    this.idleTimeout = idleTimeout;
    this.connRecycleTime = connRecycleTime;
    this.certContext =
        new JksContext(path, jksKeyPath, jksKeyPassword, jksTrustPath, jksTrustPassword);
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

  private void cleanExpireConn(Collection<ManagedChannel> pending) {
    for (ManagedChannel channel : pending) {
      logger.info("cleaning expire channels");
      channel.shutdownNow();
      while (!channel.isShutdown()) {
        try {
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
          logger.info("recycle channels timeout:", e);
        }
      }
    }
  }

  private synchronized ManagedChannel reload(String address, HostMapping mapping) throws Exception {
    assert certContext != null;
    if (certContext.isModified()) {
      logger.info("certificate file changed, reloading");

      sslContextBuilder.set(new Epoch<>(certContext.createSslContextBuilder(), epoch++));

      Collection<ManagedChannel> pending = new HashSet<>();
      for (Map.Entry<Pair<Epoch<SslContextBuilder>, String>, ManagedChannel> pair :
          connPool.entrySet()) {
        Epoch<SslContextBuilder> b = pair.getKey().getLeft();
        if (b != sslContextBuilder.get()) {
          pending.add(pair.getValue());
          connPool.remove(pair.getKey());
          connPool.computeIfAbsent(
              Pair.of(sslContextBuilder.get(), pair.getKey().getRight()),
              key -> createChannel(key.getLeft().get(), key.getRight(), mapping));
        }
      }
      recycler.schedule(() -> cleanExpireConn(pending), connRecycleTime, TimeUnit.SECONDS);

      // The `connPool` use (sslContextBuilder, address) as key, for the following reason:
      // When the `sslContextBuilder` is changed, the `connPool` will compute a new key for the
      // address. If the previous thread doesn't see the change of the sslContextBuilder, it will
      // use the old key to retrieve a broken channel without breaking the current thread's
      // computation. Otherwise, it will use the new key(with the latest sslContextBuilder) to
      // retrieve a new channel, then the current thread can still read the new channel.
      return connPool.computeIfAbsent(
          Pair.of(sslContextBuilder.get(), address),
          key -> createChannel(key.getLeft().get(), key.getRight(), mapping));
    }
    return null;
  }

  public ManagedChannel getChannel(String address, HostMapping mapping) {
    if (certContext != null) {
      try {
        ManagedChannel channel = reload(address, mapping);
        if (channel != null) {
          return channel;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return connPool.computeIfAbsent(
        Pair.of(sslContextBuilder.get(), address),
        key -> createChannel(key.getLeft().get(), key.getRight(), mapping));
  }

  public void close() {
    for (ManagedChannel ch : connPool.values()) {
      ch.shutdown();
    }
    connPool.clear();

    if (certContext != null) {
      recycler.shutdownNow();
      certContext.close();
    }
  }
}
