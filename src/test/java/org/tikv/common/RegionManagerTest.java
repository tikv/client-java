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
import static org.junit.Assert.fail;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.key.Key;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.StoreState;

public class RegionManagerTest extends PDMockServerTest {

  private RegionManager mgr;

  @Before
  @Override
  public void setup() throws IOException {
    super.setup();
    mgr = session.getRegionManager();
  }

  @Test
  public void testRegionBorder() {
    RangeMap<Key, Long> map = TreeRangeMap.create();

    map.put(KeyRangeUtils.makeRange(ByteString.EMPTY, ByteString.copyFromUtf8("abc")), 1L);
    map.put(KeyRangeUtils.makeRange(ByteString.copyFromUtf8("abc"), ByteString.EMPTY), 2L);
    assert map.get(Key.toRawKey(ByteString.EMPTY)) == null;
    assert map.get(Key.toRawKey(ByteString.EMPTY, true)) == 1L;
  }

  @Test
  public void getRegionByKey() {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});
    ByteString searchKey = ByteString.copyFrom(new byte[] {5});
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    String testAddress = "127.0.0.1";
    leader.addGetRegionListener(
        request ->
            GrpcUtils.makeGetRegionResponse(
                leader.getClusterId(),
                GrpcUtils.makeRegion(
                    regionId,
                    GrpcUtils.encodeKey(startKey.toByteArray()),
                    GrpcUtils.encodeKey(endKey.toByteArray()),
                    GrpcUtils.makeRegionEpoch(confVer, ver),
                    GrpcUtils.makePeer(1, 10),
                    GrpcUtils.makePeer(2, 20))));

    AtomicInteger i = new AtomicInteger(0);
    long[] ids = new long[] {10, 20};
    leader.addGetStoreListener(
        (request ->
            GrpcUtils.makeGetStoreResponse(
                leader.getClusterId(),
                GrpcUtils.makeStore(
                    ids[i.getAndIncrement()],
                    testAddress,
                    StoreState.Up,
                    GrpcUtils.makeStoreLabel("k1", "v1"),
                    GrpcUtils.makeStoreLabel("k2", "v2")))));

    TiRegion region = mgr.getRegionByKey(startKey);
    assertEquals(region.getId(), regionId);

    TiRegion regionToSearch = mgr.getRegionByKey(searchKey);
    assertEquals(region, regionToSearch);
  }

  @Test
  public void getStoreByKey() {
    ByteString startKey = ByteString.copyFrom(new byte[] {1});
    ByteString endKey = ByteString.copyFrom(new byte[] {10});
    ByteString searchKey = ByteString.copyFrom(new byte[] {5});
    String testAddress = "testAddress";
    long storeId = 233;
    int confVer = 1026;
    int ver = 1027;
    long regionId = 233;
    leader.addGetRegionListener(
        request ->
            GrpcUtils.makeGetRegionResponse(
                leader.getClusterId(),
                GrpcUtils.makeRegion(
                    regionId,
                    GrpcUtils.encodeKey(startKey.toByteArray()),
                    GrpcUtils.encodeKey(endKey.toByteArray()),
                    GrpcUtils.makeRegionEpoch(confVer, ver),
                    GrpcUtils.makePeer(storeId, 10),
                    GrpcUtils.makePeer(storeId + 1, 20))));

    AtomicInteger i = new AtomicInteger(0);
    long[] ids = new long[] {10, 20};
    leader.addGetStoreListener(
        (request ->
            GrpcUtils.makeGetStoreResponse(
                leader.getClusterId(),
                GrpcUtils.makeStore(
                    ids[i.getAndIncrement()],
                    testAddress,
                    StoreState.Up,
                    GrpcUtils.makeStoreLabel("k1", "v1"),
                    GrpcUtils.makeStoreLabel("k2", "v2")))));

    Pair<TiRegion, TiStore> pair = mgr.getRegionStorePairByKey(searchKey);
    assertEquals(pair.first.getId(), regionId);
    assertEquals(pair.first.getId(), storeId);
  }

  @Test
  public void getStoreById() {
    long storeId = 234;
    String testAddress = "testAddress";
    leader.addGetStoreListener(
        request ->
            GrpcUtils.makeGetStoreResponse(
                leader.getClusterId(),
                GrpcUtils.makeStore(
                    storeId,
                    testAddress,
                    Metapb.StoreState.Up,
                    GrpcUtils.makeStoreLabel("k1", "v1"),
                    GrpcUtils.makeStoreLabel("k2", "v2"))));
    TiStore store = mgr.getStoreById(storeId);
    assertEquals(store.getStore().getId(), storeId);

    leader.addGetStoreListener(
        request ->
            GrpcUtils.makeGetStoreResponse(
                leader.getClusterId(),
                GrpcUtils.makeStore(
                    storeId + 1,
                    testAddress,
                    StoreState.Tombstone,
                    GrpcUtils.makeStoreLabel("k1", "v1"),
                    GrpcUtils.makeStoreLabel("k2", "v2"))));

    try {
      mgr.getStoreById(storeId + 1);
      fail();
    } catch (Exception ignored) {
    }

    mgr.invalidateStore(storeId);
    try {
      mgr.getStoreById(storeId);
      fail();
    } catch (Exception ignored) {
    }
  }
}
