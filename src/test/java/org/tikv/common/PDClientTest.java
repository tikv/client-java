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
import static org.tikv.common.GrpcUtils.encodeKey;

import com.google.protobuf.ByteString;
import java.util.concurrent.*;
import org.junit.Test;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.Metapb.StoreState;

public class PDClientTest extends PDMockServerTest {
  private static final String LOCAL_ADDR_IPV6 = "[::]";

  @Test
  public void testCreate() throws Exception {
    try (PDClient client = session.getPDClient()) {
      assertEquals(client.getPdClientWrapper().getLeaderInfo(), LOCAL_ADDR + ":" + pdServer.port);
      assertEquals(client.getHeader().getClusterId(), CLUSTER_ID);
    }
  }

  @Test
  public void testSwitchLeader() throws Exception {
    try (PDClient client = session.getPDClient()) {
      client.trySwitchLeader("http://" + LOCAL_ADDR + ":" + (pdServer.port + 1));
      assertEquals(
          "http://" + LOCAL_ADDR + ":" + (pdServer.port + 1),
          client.getPdClientWrapper().getLeaderInfo());
    }
    tearDown();
    setUp(LOCAL_ADDR_IPV6);
    try (PDClient client = session.getPDClient()) {
      client.trySwitchLeader("http://" + LOCAL_ADDR_IPV6 + ":" + (pdServer.port + 2));
      assertEquals(
          "http://" + LOCAL_ADDR_IPV6 + ":" + (pdServer.port + 2),
          client.getPdClientWrapper().getLeaderInfo());
    }
  }

  @Test
  public void testTso() throws Exception {
    try (PDClient client = session.getPDClient()) {
      TiTimestamp ts = client.getTimestamp(defaultBackOff());
      // Test pdServer is set to generate physical == logical + 1
      assertEquals(ts.getPhysical(), ts.getLogical() + 1);
    }
  }

  @Test
  public void testGetRegionByKey() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;
    pdServer.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      Pair<Metapb.Region, Metapb.Peer> rl =
          client.getRegionByKey(defaultBackOff(), ByteString.EMPTY);
      Metapb.Region r = rl.first;
      Metapb.Peer l = rl.second;
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(l.getId(), 1);
      assertEquals(l.getStoreId(), 10);
    }
  }

  @Test
  public void testGetRegionById() throws Exception {
    byte[] startKey = new byte[] {1, 0, 2, 4};
    byte[] endKey = new byte[] {1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;

    pdServer.addGetRegionByIDResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeRegion(
                1,
                encodeKey(startKey),
                encodeKey(endKey),
                GrpcUtils.makeRegionEpoch(confVer, ver),
                GrpcUtils.makePeer(1, 10),
                GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      Pair<Metapb.Region, Metapb.Peer> rl = client.getRegionByID(defaultBackOff(), 0);
      Metapb.Region r = rl.first;
      Metapb.Peer l = rl.second;
      assertEquals(r.getStartKey(), ByteString.copyFrom(startKey));
      assertEquals(r.getEndKey(), ByteString.copyFrom(endKey));
      assertEquals(r.getRegionEpoch().getConfVer(), confVer);
      assertEquals(r.getRegionEpoch().getVersion(), ver);
      assertEquals(l.getId(), 1);
      assertEquals(l.getStoreId(), 10);
    }
  }

  @Test
  public void testGetStore() throws Exception {
    long storeId = 1;
    String testAddress = "testAddress";
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                storeId,
                testAddress,
                Metapb.StoreState.Up,
                GrpcUtils.makeStoreLabel("k1", "v1"),
                GrpcUtils.makeStoreLabel("k2", "v2"))));
    try (PDClient client = session.getPDClient()) {
      Store r = client.getStore(defaultBackOff(), 0);
      assertEquals(r.getId(), storeId);
      assertEquals(r.getAddress(), testAddress);
      assertEquals(r.getState(), Metapb.StoreState.Up);
      assertEquals(r.getLabels(0).getKey(), "k1");
      assertEquals(r.getLabels(1).getKey(), "k2");
      assertEquals(r.getLabels(0).getValue(), "v1");
      assertEquals(r.getLabels(1).getValue(), "v2");

      pdServer.addGetStoreResp(
          GrpcUtils.makeGetStoreResponse(
              pdServer.getClusterId(),
              GrpcUtils.makeStore(storeId, testAddress, Metapb.StoreState.Tombstone)));
      assertEquals(StoreState.Tombstone, client.getStore(defaultBackOff(), 0).getState());
    }
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(100);
  }

  @Test
  public void testRetryPolicy() throws Exception {
    long storeId = 1024;
    ExecutorService service = Executors.newCachedThreadPool();
    pdServer.addGetStoreResp(null);
    pdServer.addGetStoreResp(null);
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(), GrpcUtils.makeStore(storeId, "", Metapb.StoreState.Up)));
    try (PDClient client = session.getPDClient()) {
      Callable<Store> storeCallable =
          () -> client.getStore(ConcreteBackOffer.newCustomBackOff(5000), 0);
      Future<Store> storeFuture = service.submit(storeCallable);
      try {
        Store r = storeFuture.get(50, TimeUnit.SECONDS);
        assertEquals(r.getId(), storeId);
      } catch (TimeoutException e) {
        fail();
      }

      // Should fail
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);
      pdServer.addGetStoreResp(null);

      pdServer.addGetStoreResp(
          GrpcUtils.makeGetStoreResponse(
              pdServer.getClusterId(), GrpcUtils.makeStore(storeId, "", Metapb.StoreState.Up)));
      try {
        client.getStore(defaultBackOff(), 0);
      } catch (GrpcException e) {
        assertTrue(true);
        return;
      }
      fail();
    }
  }
}
