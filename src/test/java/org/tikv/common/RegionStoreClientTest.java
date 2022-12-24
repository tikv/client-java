/*
 * Copyright 2017 TiKV Project Authors.
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

package org.tikv.common;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.KeyException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Errorpb.EpochNotMatch;
import org.tikv.kvproto.Errorpb.NotLeader;
import org.tikv.kvproto.Errorpb.ServerIsBusy;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;

public class RegionStoreClientTest extends MockServerTest {
  private static final Logger logger = LoggerFactory.getLogger(MockServerTest.class);

  private RegionStoreClient createClientV2() {
    return createClient("2.1.19");
  }

  private RegionStoreClient createClientV3() {
    return createClient("3.0.12");
  }

  private RegionStoreClient createClientV4() {
    return createClient("6.1.0");
  }

  private RegionStoreClient createClient(String version) {
    Metapb.Store meta =
        Metapb.Store.newBuilder()
            .setAddress(LOCAL_ADDR + ":" + port)
            .setId(1)
            .setState(Metapb.StoreState.Up)
            .setVersion(version)
            .build();
    TiStore store = new TiStore(meta);

    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(
            session.getConf(),
            session.getChannelFactory(),
            new RegionManager(session.getConf(), session.getPDClient()),
            session.getPDClient());

    return builder.build(region, store);
  }

  @Test
  public void rawGetTest() {
    doRawGetTest(createClientV3());
  }

  public void doRawGetTest(RegionStoreClient client) {
    server.put("key1", "value1");
    Optional<ByteString> value = client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("key1"));
    assertEquals(ByteString.copyFromUtf8("value1"), value.get());

    server.putError(
        "error1", () -> Errorpb.Error.newBuilder().setNotLeader(NotLeader.getDefaultInstance()));
    // since not_leader is retryable, so the result should be correct.
    value = client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("key1"));
    assertEquals(ByteString.copyFromUtf8("value1"), value.get());

    server.putError(
        "failure",
        () -> Errorpb.Error.newBuilder().setEpochNotMatch(EpochNotMatch.getDefaultInstance()));
    try {
      // since stale epoch is not retryable, so the test should fail.
      client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("failure"));
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  @Test
  public void getTest() throws Exception {
    doGetTest(createClientV3());
  }

  public void doGetTest(RegionStoreClient client) {
    server.put("key1", "value1");
    ByteString value = client.get(defaultBackOff(), ByteString.copyFromUtf8("key1"), 1);
    assertEquals(ByteString.copyFromUtf8("value1"), value);

    server.putError(
        "error1",
        () -> Errorpb.Error.newBuilder().setServerIsBusy(ServerIsBusy.getDefaultInstance()));
    try {
      client.get(defaultBackOff(), ByteString.copyFromUtf8("error1"), 1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  @Test
  public void batchGetTest() {
    doBatchGetTest(createClientV3());
  }

  public void doBatchGetTest(RegionStoreClient client) {
    server.put("key1", "value1");
    server.put("key2", "value2");
    server.put("key4", "value4");
    server.put("key5", "value5");
    List<Kvrpcpb.KvPair> kvs =
        client.batchGet(
            defaultBackOff(),
            ImmutableList.of(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key2")),
            1);
    assertEquals(2, kvs.size());
    kvs.forEach(
        kv ->
            assertEquals(
                kv.getKey().toStringUtf8().replace("key", "value"), kv.getValue().toStringUtf8()));

    server.putError(
        "error1",
        () -> Errorpb.Error.newBuilder().setServerIsBusy(ServerIsBusy.getDefaultInstance()));
    try {
      client.batchGet(
          defaultBackOff(),
          ImmutableList.of(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("error1")),
          1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  @Test
  public void scanTest() {
    doScanTest(createClientV4());
  }

  public void doScanTest(RegionStoreClient client) {
    TiTimestamp startTs = session.getTimestamp();

    server.put("key1", "value1");
    server.put("key2", "value2");
    server.put("key4", "value4");
    server.put("key5", "value5");
    List<Kvrpcpb.KvPair> kvs = client.scan(defaultBackOff(), ByteString.copyFromUtf8("key2"), startTs.getVersion());
    assertEquals(3, kvs.size());
    kvs.forEach(
        kv ->
            assertEquals(
                kv.getKey().toStringUtf8().replace("key", "value"), kv.getValue().toStringUtf8()));

    // put region error
    server.putError(
        "error1",
        () -> Errorpb.Error.newBuilder().setServerIsBusy(ServerIsBusy.getDefaultInstance()));
    try {
      client.scan(defaultBackOff(), ByteString.copyFromUtf8("error1"), startTs.getVersion());
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.removeError("error1");

    // put key error
    server.putWithLock("key6", "value6", "key6", startTs.getVersion(), 3000L);
    server.putTxnStatus(startTs.getVersion(), 0L, ByteString.copyFromUtf8("key6"));
    assertTrue(server.hasLock(ByteString.copyFromUtf8("key6")));
    try {
      client.scan(defaultBackOff(), ByteString.copyFromUtf8("key2"), startTs.getVersion());
      fail();
    } catch (Exception e) {
      KeyException keyException = (KeyException) e.getCause();
      assertTrue(keyException.getMessage().contains("org.tikv.txn.Lock"));
    }
    assertTrue(server.hasLock(ByteString.copyFromUtf8("key6")));

    server.clearAllMap();
    client.close();
  }

  @Test
  public void resolveLocksTest() {
    doResolveLocksTest(createClientV4());
  }

  public void doResolveLocksTest(RegionStoreClient client) {
    ByteString primaryKey = ByteString.copyFromUtf8("primary");
    server.put(primaryKey, ByteString.copyFromUtf8("value0"));

    // get with committed lock
    {
      TiTimestamp startTs = session.getTimestamp();
      TiTimestamp commitTs = session.getTimestamp();
      logger.info("startTs: " + startTs);

      ByteString key1 = ByteString.copyFromUtf8("key1");
      ByteString value1 = ByteString.copyFromUtf8("value1");
      server.putWithLock(key1, value1, primaryKey, startTs.getVersion(), 1L);
      server.putTxnStatus(startTs.getVersion(), commitTs.getVersion());
      assertTrue(server.hasLock(key1));

      ByteString expected1 = client.get(defaultBackOff(), key1, 200);
      assertEquals(value1, expected1);
      assertFalse(server.hasLock(key1));
    }

    // get with not expired lock.
    {
      TiTimestamp startTs = session.getTimestamp();
      logger.info("startTs: " + startTs);

      ByteString key2 = ByteString.copyFromUtf8("key2");
      ByteString value2 = ByteString.copyFromUtf8("value2");
      server.putWithLock(key2, value2, key2, startTs.getVersion(), 3000L);
      server.putTxnStatus(startTs.getVersion(), 0L, key2);
      assertTrue(server.hasLock(key2));

      try {
        client.get(defaultBackOff(), key2, session.getTimestamp().getVersion());
        fail();
      } catch (Exception e) {
        KeyException keyException = (KeyException) e.getCause();
        assertTrue(keyException.getMessage().contains("org.tikv.txn.Lock"));
      }
      assertTrue(server.hasLock(key2));
    }

    // get with expired lock.
    {
      TiTimestamp startTs = session.getTimestamp();
      logger.info("startTs: " + startTs);

      ByteString key3 = ByteString.copyFromUtf8("key3");
      ByteString value3 = ByteString.copyFromUtf8("value3");
      server.putWithLock(key3, value3, key3, startTs.getVersion(), 100L);
      server.putTxnStatus(startTs.getVersion(), 0L, key3);
      assertTrue(server.hasLock(key3));

      ByteString expected3 = client.get(defaultBackOff(), key3, session.getTimestamp().getVersion());
      assertEquals(expected3, value3);
      assertFalse(server.hasLock(key3));
    }

    server.clearAllMap();
    client.close();
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }
}
