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

package org.tikv.common;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Test;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;

public class RegionStoreClientTest extends MockServerTest {

  private RegionStoreClient createClient() {
    Metapb.Store store =
        Metapb.Store.newBuilder()
            .setAddress(LOCAL_ADDR + ":" + port)
            .setId(1)
            .setState(Metapb.StoreState.Up)
            .build();

    return RegionStoreClient.create(region, store, session);
  }

  @Test
  public void rawGetTest() throws Exception {
    RegionStoreClient client = createClient();
    server.put("key1", "value1");
    ByteString value = client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("key1"));
    assertEquals(ByteString.copyFromUtf8("value1"), value);

    server.putError("error1", KVMockServer.NOT_LEADER);
    // since not_leader is retryable, so the result should be correct.
    value = client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("key1"));
    assertEquals(ByteString.copyFromUtf8("value1"), value);

    server.putError("failure", KVMockServer.STALE_EPOCH);
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
    RegionStoreClient client = createClient();
    server.put("key1", "value1");
    ByteString value = client.get(defaultBackOff(), ByteString.copyFromUtf8("key1"), 1);
    assertEquals(ByteString.copyFromUtf8("value1"), value);

    server.putError("error1", KVMockServer.ABORT);
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
  public void batchGetTest() throws Exception {
    RegionStoreClient client = createClient();

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

    server.putError("error1", KVMockServer.ABORT);
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
  public void scanTest() throws Exception {
    RegionStoreClient client = createClient();

    server.put("key1", "value1");
    server.put("key2", "value2");
    server.put("key4", "value4");
    server.put("key5", "value5");
    List<Kvrpcpb.KvPair> kvs = client.scan(defaultBackOff(), ByteString.copyFromUtf8("key2"), 1);
    assertEquals(3, kvs.size());
    kvs.forEach(
        kv ->
            assertEquals(
                kv.getKey().toStringUtf8().replace("key", "value"), kv.getValue().toStringUtf8()));

    server.putError("error1", KVMockServer.ABORT);
    try {
      client.scan(defaultBackOff(), ByteString.copyFromUtf8("error1"), 1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }
}
