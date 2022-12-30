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
import org.junit.Assert;
import org.junit.Test;
import org.tikv.kvproto.Errorpb.Error;
import org.tikv.raw.RawKVClient;

public class TsoBatchUsedUpTest extends MockThreeStoresTest {
  RawKVClient createClient() {
    return session.createRawClient();
  }

  @Test
  public void testTsoBatchUsedUp() {
    ByteString key = ByteString.copyFromUtf8("tso");
    servers.get(0).putError("tso", () -> Error.newBuilder().setMessage("TsoBatchUsedUp"));
    try (RawKVClient client = createClient()) {
      try {
        client.put(key, ByteString.EMPTY);
        Assert.fail();
      } catch (Exception ignore) {
      }
      pdServers.get(0).addGetRegionListener(request -> null);
      // Will not clean region cache
      Assert.assertNotNull(session.getRegionManager().getRegionByKey(key));
    }
  }
}
