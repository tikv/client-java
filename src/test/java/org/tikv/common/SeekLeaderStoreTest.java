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

import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.KVMockServer.State;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.StoreState;
import org.tikv.kvproto.Pdpb;
import org.tikv.raw.RawKVClient;

public class SeekLeaderStoreTest extends MockThreeStoresTest {

  private RawKVClient createClient() {
    return session.createRawClient();
  }

  @Test
  public void testSeekLeader() {
    RawKVClient client = createClient();
    ByteString key = ByteString.copyFromUtf8("key");
    ByteString value = ByteString.copyFromUtf8("value");

    put(key, value);

    Assert.assertEquals(value, client.get(key));
    servers.get(0).setState(State.Fail);
    servers.get(1).setRegion(region.switchPeer(stores.get(1).getId()));
    Assert.assertEquals(value, client.get(key));

    remove(key, value);
  }

  @Test
  public void testSeekLeaderMeetInvalidStore() {
    RawKVClient client = createClient();
    ByteString key = ByteString.copyFromUtf8("key");
    ByteString value = ByteString.copyFromUtf8("value");

    put(key, value);

    servers.get(0).setState(State.Fail);
    servers.get(2).setRegion(region.switchPeer(stores.get(2).getId()));

    AtomicInteger i = new AtomicInteger(0);
    leader.addGetStoreListener(
        request -> {
          Metapb.Store.Builder storeBuilder =
              Metapb.Store.newBuilder().mergeFrom(stores.get((int) request.getStoreId() - 1));
          if (request.getStoreId() == 0x2 && i.incrementAndGet() > 0) {
            storeBuilder.setState(StoreState.Tombstone);
          }
          return Pdpb.GetStoreResponse.newBuilder().setStore(storeBuilder.build()).build();
        });

    Assert.assertEquals(value, client.get(key));

    remove(key, value);
  }
}
