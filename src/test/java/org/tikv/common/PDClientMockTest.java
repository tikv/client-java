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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.tikv.common.MockRequestInterceptor.MockRequestInterceptorBuilder;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.Metapb.StoreState;
import org.tikv.kvproto.Pdpb.GetStoreRequest;
import org.tikv.kvproto.Pdpb.GetStoreResponse;

public class PDClientMockTest extends PDMockServerTest {

  private static final String LOCAL_ADDR_IPV6 = "[::]";
  public static final String HTTP = "http://";

  @Test
  public void testCreate() throws Exception {
    try (PDClient client = session.getPDClient()) {
      assertEquals(LOCAL_ADDR + ":" + leader.port, client.getPdClientWrapper().getLeaderInfo());
      assertEquals(CLUSTER_ID, client.getHeader().getClusterId());
    }
  }

  @Test
  public void testSwitchLeader() throws Exception {
    try (PDClient client = session.getPDClient()) {
      client.trySwitchLeader(HTTP + LOCAL_ADDR + ":" + (leader.port + 1));
      assertEquals(
          client.getPdClientWrapper().getLeaderInfo(), HTTP + LOCAL_ADDR + ":" + (leader.port + 1));
    }
    tearDown();
    setup(LOCAL_ADDR_IPV6);
    try (PDClient client = session.getPDClient()) {
      client.trySwitchLeader(HTTP + LOCAL_ADDR_IPV6 + ":" + (leader.port + 2));
      assertEquals(
          client.getPdClientWrapper().getLeaderInfo(),
          HTTP + LOCAL_ADDR_IPV6 + ":" + (leader.port + 2));
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
    byte[] startKey = new byte[]{1, 0, 2, 4};
    byte[] endKey = new byte[]{1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;
    leader.addGetRegionInterceptor(
        request ->
            GrpcUtils.makeGetRegionResponse(
                leader.getClusterId(),
                GrpcUtils.makeRegion(
                    1,
                    ByteString.copyFrom(startKey),
                    ByteString.copyFrom(endKey),
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
      assertEquals(1, l.getId());
      assertEquals(10, l.getStoreId());
    }
  }

  @Test
  public void testGetRegionById() throws Exception {
    byte[] startKey = new byte[]{1, 0, 2, 4};
    byte[] endKey = new byte[]{1, 0, 2, 5};
    int confVer = 1026;
    int ver = 1027;

    leader.addGetRegionByIdInterceptor(
        request ->
            GrpcUtils.makeGetRegionResponse(
                leader.getClusterId(),
                GrpcUtils.makeRegion(
                    1,
                    ByteString.copyFrom(startKey),
                    ByteString.copyFrom(endKey),
                    GrpcUtils.makeRegionEpoch(confVer, ver),
                    GrpcUtils.makePeer(1, 10),
                    GrpcUtils.makePeer(2, 20))));
    try (PDClient client = session.getPDClient()) {
      Pair<Metapb.Region, Metapb.Peer> rl = client.getRegionByID(defaultBackOff(), 0);
      Metapb.Region r = rl.first;
      Metapb.Peer l = rl.second;
      assertEquals(ByteString.copyFrom(startKey), r.getStartKey());
      assertEquals(ByteString.copyFrom(endKey), r.getEndKey());
      assertEquals(confVer, r.getRegionEpoch().getConfVer());
      assertEquals(ver, r.getRegionEpoch().getVersion());
      assertEquals(1, l.getId());
      assertEquals(10, l.getStoreId());
    }
  }

  @Test
  public void testGetStore() throws Exception {
    long storeId = 1;
    String testAddress = "testAddress";
    leader.addGetStoreInterceptor(
        request ->
            GrpcUtils.makeGetStoreResponse(
                leader.getClusterId(),
                GrpcUtils.makeStore(
                    storeId,
                    testAddress,
                    Metapb.StoreState.Up,
                    GrpcUtils.makeStoreLabel("k1", "v1"),
                    GrpcUtils.makeStoreLabel("k2", "v2"))));
    try (PDClient client = session.getPDClient()) {
      Store r = client.getStore(defaultBackOff(), storeId);
      assertEquals(storeId, r.getId());
      assertEquals(testAddress, r.getAddress());
      assertEquals(Metapb.StoreState.Up, r.getState());
      assertEquals("k1", r.getLabels(0).getKey());
      assertEquals("k2", r.getLabels(1).getKey());
      assertEquals("v1", r.getLabels(0).getValue());
      assertEquals("v2", r.getLabels(1).getValue());

      leader.addGetStoreInterceptor(
          request ->
              GrpcUtils.makeGetStoreResponse(
                  leader.getClusterId(),
                  GrpcUtils.makeStore(storeId, testAddress, Metapb.StoreState.Tombstone)));
      assertEquals(StoreState.Tombstone, client.getStore(defaultBackOff(), storeId).getState());
    }
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }

  @Test
  public void testRetryPolicy() throws Exception {
    long storeId = 1024;
    ExecutorService service = Executors.newCachedThreadPool();
    AtomicInteger i = new AtomicInteger();
    Interceptor<GetStoreRequest, GetStoreResponse> interceptor =
        new MockRequestInterceptorBuilder<GetStoreRequest, GetStoreResponse>()
            .withHandler(
                request -> {
                  if (i.getAndIncrement() < 2) {
                    return null;
                  } else {
                    return GrpcUtils.makeGetStoreResponse(
                        leader.getClusterId(), GrpcUtils.makeStore(storeId, "", StoreState.Up));
                  }
                })
            .build();
    leader.addGetStoreInterceptor(interceptor);

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
      AtomicInteger j = new AtomicInteger();
      interceptor =
          new MockRequestInterceptorBuilder<GetStoreRequest, GetStoreResponse>()
              .withHandler(
                  request -> {
                    if (j.getAndIncrement() < 6) {
                      return null;
                    } else {
                      return GrpcUtils.makeGetStoreResponse(
                          leader.getClusterId(),
                          GrpcUtils.makeStore(storeId, "", Metapb.StoreState.Up));
                    }
                  })
              .build();
      leader.addGetStoreInterceptor(interceptor);

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
