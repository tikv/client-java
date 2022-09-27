/*
 * Copyright 2022 TiKV Project Authors.
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

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.Test;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Errorpb.EpochNotMatch;
import org.tikv.kvproto.Errorpb.NotLeader;
import org.tikv.kvproto.Errorpb.StoreNotMatch;
import org.tikv.kvproto.Metapb;

public class CacheInvalidCallBackTest extends MockServerTest {

  private RegionStoreClient createClient(
      String version, Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack) {
    Metapb.Store meta =
        Metapb.Store.newBuilder()
            .setAddress(LOCAL_ADDR + ":" + port)
            .setId(1)
            .setState(Metapb.StoreState.Up)
            .setVersion(version)
            .build();
    TiStore store = new TiStore(meta);

    RegionManager manager = new RegionManager(session.getConf(), session.getPDClient());
    manager.addCacheInvalidateCallback(cacheInvalidateCallBack);
    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(
            session.getConf(), session.getChannelFactory(), manager, session.getPDClient());

    return builder.build(region, store);
  }

  @Test
  public void testcacheInvalidCallBack() {
    String version = "3.0.12";
    CacheInvalidateCallBack cacheInvalidateCallBack = new CacheInvalidateCallBack();
    doRawGetTest(createClient(version, cacheInvalidateCallBack), cacheInvalidateCallBack);
  }

  public void doRawGetTest(
      RegionStoreClient client, CacheInvalidateCallBack cacheInvalidateCallBack) {
    server.put("key1", "value1");
    Optional<ByteString> value = client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("key1"));
    assertEquals(ByteString.copyFromUtf8("value1"), value.get());
    try {
      server.putError(
          "error1", () -> Errorpb.Error.newBuilder().setNotLeader(NotLeader.getDefaultInstance()));
      client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("error1"));
      fail();
    } catch (Exception e) {
      assertEquals(1, cacheInvalidateCallBack.cacheInvalidateEvents.size());
    }

    server.putError(
        "failure",
        () -> Errorpb.Error.newBuilder().setEpochNotMatch(EpochNotMatch.getDefaultInstance()));
    try {
      client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("failure"));
      fail();
    } catch (Exception e) {
      assertEquals(2, cacheInvalidateCallBack.cacheInvalidateEvents.size());
    }
    server.putError(
        "store_not_match",
        () -> Errorpb.Error.newBuilder().setStoreNotMatch(StoreNotMatch.getDefaultInstance()));
    try {
      client.rawGet(defaultBackOff(), ByteString.copyFromUtf8("failure"));
      fail();
    } catch (Exception e) {
      assertEquals(3, cacheInvalidateCallBack.cacheInvalidateEvents.size());
    }
    server.clearAllMap();
    client.close();
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }

  static class CacheInvalidateCallBack implements Function<CacheInvalidateEvent, Void> {

    public List<CacheInvalidateEvent> cacheInvalidateEvents = new ArrayList<>();

    @Override
    public Void apply(CacheInvalidateEvent cacheInvalidateEvent) {
      cacheInvalidateEvents.add(cacheInvalidateEvent);
      return null;
    }
  }
}
