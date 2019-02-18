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

package org.tikv.txn;

import static junit.framework.TestCase.*;
import static org.tikv.common.util.BackOffFunction.BackOffFuncType.BoTxnLock;

import com.google.protobuf.ByteString;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.PDClient;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.KeyException;
import org.tikv.common.exception.RegionException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.operation.KVErrorHandler;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb.*;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.TikvGrpc;

public class LockResolverTest {
  private TiSession session;
  private static final int DefaultTTL = 10;
  private boolean init = false;
  private BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);
  private ReadOnlyPDClient pdClient;

  private void putKV(String key, String value, long startTS, long commitTS) {
    Mutation m =
        Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setOp(Op.Put)
            .setValue(ByteString.copyFromUtf8(value))
            .build();

    boolean res = prewrite(Arrays.asList(m), startTS, m);
    assertTrue(res);
    res = commit(startTS, commitTS, Arrays.asList(ByteString.copyFromUtf8(key)));
    assertTrue(res);
  }

  private boolean prewrite(List<Mutation> mutations, long startTS, Mutation primary) {
    if (mutations.size() == 0) return true;

    for (Mutation m : mutations) {
      RegionStoreClient client = builder.build(m.getKey());
      TiRegion region = regionManager.getRegionByKey(m.getKey());
      Supplier<PrewriteRequest> factory =
          () ->
              PrewriteRequest.newBuilder()
                  .addAllMutations(Arrays.asList(m))
                  .setPrimaryLock(primary.getKey())
                  .setStartVersion(startTS)
                  .setLockTtl(DefaultTTL)
                  .setContext(region.getContext())
                  .build();

      KVErrorHandler<PrewriteResponse> handler =
          new KVErrorHandler<>(
                  regionManager,
                  client,
                  region,
                  resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      PrewriteResponse resp =
          client.callWithRetry(backOffer, TikvGrpc.METHOD_KV_PREWRITE, factory, handler);

      if (resp.hasRegionError()) {
        throw new RegionException(resp.getRegionError());
      }

      if (resp.getErrorsCount() == 0) {
        continue;
      }

      List<Lock> locks = new ArrayList<>();
      for (KeyError err : resp.getErrorsList()) {
        if (err.hasLocked()) {
          Lock lock = new Lock(err.getLocked());
          locks.add(lock);
        } else {
          throw new KeyException(err);
        }
      }

      LockResolverClient resolver = null;
      try {
        Field field = RegionStoreClient.class.getDeclaredField("lockResolverClient");
        assert (field != null);
        field.setAccessible(true);
        resolver = (LockResolverClient) (field.get(client));
      } catch (Exception e) {
        fail();
      }

      assertNotNull(resolver);

      if (!resolver.resolveLocks(backOffer, locks)) {
        backOffer.doBackOff(BoTxnLock, new KeyException(resp.getErrorsList().get(0)));
      }

      prewrite(Arrays.asList(m), startTS, primary);
    }

    return true;
  }

  private boolean lockKey(
      String key,
      String value,
      String primaryKey,
      String primaryValue,
      boolean commitPrimary,
      long startTs,
      long commitTS) {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(primaryKey))
            .setValue(ByteString.copyFromUtf8(primaryValue))
            .setOp(Op.Put)
            .build());
    if (!key.equals(primaryKey)) {
      mutations.add(
          Mutation.newBuilder()
              .setKey(ByteString.copyFromUtf8(key))
              .setValue(ByteString.copyFromUtf8(value))
              .setOp(Op.Put)
              .build());
    }
    if (!prewrite(mutations, startTs, mutations.get(0))) return false;

    if (commitPrimary) {
      if (!key.equals(primaryKey)) {
        if (!commit(
            startTs,
            commitTS,
            Arrays.asList(ByteString.copyFromUtf8(primaryKey), ByteString.copyFromUtf8(key)))) {
          return false;
        }
      } else {
        if (!commit(startTs, commitTS, Arrays.asList(ByteString.copyFromUtf8(primaryKey)))) {
          return false;
        }
      }
    }

    return true;
  }

  private boolean commit(long startTS, long commitTS, List<ByteString> keys) {
    if (keys.size() == 0) return true;

    for (ByteString k : keys) {
      RegionStoreClient client = builder.build(k);
      TiRegion region = regionManager.getRegionByKey(k);
      Supplier<CommitRequest> factory =
          () ->
              CommitRequest.newBuilder()
                  .setStartVersion(startTS)
                  .setCommitVersion(commitTS)
                  .addAllKeys(Arrays.asList(k))
                  .setContext(region.getContext())
                  .build();

      KVErrorHandler<CommitResponse> handler =
          new KVErrorHandler<>(
                  this.regionManager,
                  client,
                  region,
                  resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      CommitResponse resp =
          client.callWithRetry(backOffer, TikvGrpc.METHOD_KV_COMMIT, factory, handler);

      if (resp.hasRegionError()) {
        throw new RegionException(resp.getRegionError());
      }

      if (resp.hasError()) {
        throw new KeyException(resp.getError());
      }
    }
    return true;
  }

  private void putAlphabet() {
    for (int i = 0; i < 26; i++) {
      long startTs = pdClient.getTimestamp(backOffer).getVersion();
      long endTs = pdClient.getTimestamp(backOffer).getVersion();
      while (startTs == endTs) {
        endTs = pdClient.getTimestamp(backOffer).getVersion();
      }
      putKV(String.valueOf((char) ('a' + i)), String.valueOf((char) ('a' + i)), startTs, endTs);
    }
    for (int i = 0; i < 26; i++) {
      ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a' + i)));
      RegionStoreClient client = builder.build(key);
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i)));
    }
  }

  private void prepareAlphabetLocks() {
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);
    while (startTs == endTs) {
      endTs = pdClient.getTimestamp(backOffer);
    }
    putKV("c", "cc", startTs.getVersion(), endTs.getVersion());
    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);
    while (startTs == endTs) {
      endTs = pdClient.getTimestamp(backOffer);
    }

    assertTrue(lockKey("c", "c", "z1", "z1", true, startTs.getVersion(), endTs.getVersion()));
    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);
    while (startTs == endTs) {
      endTs = pdClient.getTimestamp(backOffer);
    }
    assertTrue(lockKey("d", "dd", "z2", "z2", false, startTs.getVersion(), endTs.getVersion()));
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }

  private class RetryException extends RuntimeException {
    public RetryException() {}
  }

  private RegionStoreClient.RegionStoreClientBuilder builder;
  private RegionManager regionManager;
  @Before
  public void setUp() throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    ChannelFactory channelFactory = new ChannelFactory(conf.getMaxFrameSize());
    session = TiSession.create(conf);
    try {
      pdClient = session.getPDClient();
      this.regionManager = new RegionManager(pdClient);
      this.builder =
              new RegionStoreClient.RegionStoreClientBuilder(conf, channelFactory, regionManager);
    } catch (Exception e) {
      init = false;
    }
  }

  @Test
  public void getSITest() throws Exception {
    if (!init) {
      System.out.println("PD client not initialized. Test skipped");
      return;
    }
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    putAlphabet();
    prepareAlphabetLocks();

    for (int i = 0; i < 26; i++) {
      ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a' + i)));
      RegionStoreClient client = builder.build(key);
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i)));
    }

    session.getConf().setIsolationLevel(IsolationLevel.RC);
  }

  @Test
  public void getRCTest() {
    if (!init) {
      System.out.println("PD client not initialized. Test skipped");
      return;
    }
    session.getConf().setIsolationLevel(IsolationLevel.RC);
    putAlphabet();
    prepareAlphabetLocks();

    for (int i = 0; i < 26; i++) {
      ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a' + i)));
      RegionStoreClient client = builder.build(key);
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i)));
    }
  }

  @Test
  public void cleanLockTest() {
    if (!init) {
      System.out.println("PD client not initialized. Test skipped");
      return;
    }
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    for (int i = 0; i < 26; i++) {
      String k = String.valueOf((char) ('a' + i));
      TiTimestamp startTs = pdClient.getTimestamp(backOffer);
      TiTimestamp endTs = pdClient.getTimestamp(backOffer);
      lockKey(k, k, k, k, false, startTs.getVersion(), endTs.getVersion());
    }

    List<Mutation> mutations = new ArrayList<>();
    List<ByteString> keys = new ArrayList<>();
    for (int i = 0; i < 26; i++) {
      String k = String.valueOf((char) ('a' + i));
      String v = String.valueOf((char) ('a' + i + 1));
      Mutation m =
          Mutation.newBuilder()
              .setKey(ByteString.copyFromUtf8(k))
              .setOp(Op.Put)
              .setValue(ByteString.copyFromUtf8(v))
              .build();
      mutations.add(m);
      keys.add(ByteString.copyFromUtf8(k));
    }

    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    boolean res = prewrite(mutations, startTs.getVersion(), mutations.get(0));
    assertTrue(res);
    res = commit(startTs.getVersion(), endTs.getVersion(), keys);
    assertTrue(res);

    for (int i = 0; i < 26; i++) {
      ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a' + i)));
      RegionStoreClient client = builder.build(key);
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i + 1)));
    }

    session.getConf().setIsolationLevel(IsolationLevel.RC);
  }

  @Test
  public void txnStatusTest() {
    if (!init) {
      System.out.println("PD client not initialized. Test skipped");
      return;
    }
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());
    ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a')));
    RegionStoreClient client = builder.build(key);

    long status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf((char) ('a'))));
    assertEquals(status, endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "a", "a", "a", true, startTs.getVersion(), endTs.getVersion());
    key = ByteString.copyFromUtf8(String.valueOf((char) ('a')));
    client = builder.build(key);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf((char) ('a'))));
    assertEquals(status, endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "a", "a", "a", false, startTs.getVersion(), endTs.getVersion());

    key = ByteString.copyFromUtf8(String.valueOf((char) ('a')));
    client = builder.build(key);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf((char) ('a'))));
    assertNotSame(status, endTs.getVersion());

    session.getConf().setIsolationLevel(IsolationLevel.RC);
  }

  @Test
  public void SITest() {
    if (!init) {
      System.out.println("PD client not initialized. Test skipped");
      return;
    }
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "aa", "a", "aa", false, startTs.getVersion(), endTs.getVersion());

    ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a')));
    RegionStoreClient client = builder.build(key);

    ByteString v =
        client.get(
            backOffer,
            ByteString.copyFromUtf8(String.valueOf((char) ('a'))),
            pdClient.getTimestamp(backOffer).getVersion());
    assertEquals(v.toStringUtf8(), String.valueOf((char) ('a')));

    try {
      commit(startTs.getVersion(), endTs.getVersion(), Arrays.asList(ByteString.copyFromUtf8("a")));
      fail();
    } catch (KeyException e) {
      assertNotNull(e.getKeyErr().getRetryable());
    }
    session.getConf().setIsolationLevel(IsolationLevel.RC);
  }

  @Test
  public void RCTest() {
    if (!init) {
      System.out.println("PD client not initialized. Test skipped");
      return;
    }
    session.getConf().setIsolationLevel(IsolationLevel.RC);
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "aa", "a", "aa", false, startTs.getVersion(), endTs.getVersion());

    ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a')));
    RegionStoreClient client = builder.build(key);

    ByteString v =
        client.get(
            backOffer,
            ByteString.copyFromUtf8(String.valueOf((char) ('a'))),
            pdClient.getTimestamp(backOffer).getVersion());
    assertEquals(v.toStringUtf8(), String.valueOf((char) ('a')));

    try {
      commit(startTs.getVersion(), endTs.getVersion(), Arrays.asList(ByteString.copyFromUtf8("a")));
    } catch (KeyException e) {
      fail();
    }
  }

  private static Coprocessor.KeyRange createByteStringRange(ByteString sKey, ByteString eKey) {
    return Coprocessor.KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }
}
