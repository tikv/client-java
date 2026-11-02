/*
 * Copyright 2026 TiKV Project Authors.
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

package org.tikv.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.kvproto.Kvrpcpb.KvPair;

/** End-to-end verification of RawKVAsyncClient against a local tiup playground. */
public class RawKVAsyncPlaygroundTest {

  @Test
  public void testPlayground() throws Exception {
    if (!isPlaygroundUp()) {
      System.out.println(
          "Skip RawKVAsyncPlaygroundTest: local playground (127.0.0.1:2379) is not running");
      return;
    }
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    conf.setWarmUpEnable(false);
    conf.setEnableAtomicForCAS(true);
    try (TiSession session = TiSession.create(conf);
        RawKVAsyncClient client = session.createRawAsyncClient()) {

      // 1. put + get
      ByteString k = ByteString.copyFromUtf8("demo-key");
      ByteString v1 = ByteString.copyFromUtf8("demo-value-1");
      client.putAsync(k, v1).join();
      Optional<ByteString> got = client.getAsync(k).join();
      assertTrue(got.isPresent());
      assertEquals(v1, got.get());

      // 2. putIfAbsent
      Optional<ByteString> existing =
          client.putIfAbsentAsync(k, ByteString.copyFromUtf8("other")).join();
      assertTrue(existing.isPresent());
      assertEquals(v1, existing.get());

      // 3. compareAndSet
      ByteString v2 = ByteString.copyFromUtf8("demo-value-2");
      client.compareAndSetAsync(k, Optional.of(v1), v2).join();
      assertEquals(v2, client.getAsync(k).join().get());

      // 4. scan ordering
      // scan covers [startKey, +inf) and the playground may hold keys left by other
      // tests, so assert only the keys with the scan-async- prefix
      ByteString scanPrefix = ByteString.copyFromUtf8("scan-async-");
      for (int i = 0; i < 100; i++) {
        client
            .putAsync(
                ByteString.copyFromUtf8("scan-async-" + String.format("%04d", i)),
                ByteString.copyFromUtf8("sv-" + i))
            .join();
      }
      List<KvPair> scanned =
          client
              .scanAsync(scanPrefix, 1000)
              .join()
              .stream()
              .filter(kv -> kv.getKey().toStringUtf8().startsWith("scan-async-"))
              .collect(Collectors.toList());
      assertEquals(100, scanned.size());
      for (int i = 0; i < scanned.size(); i++) {
        assertEquals(
            ByteString.copyFromUtf8("scan-async-" + String.format("%04d", i)),
            scanned.get(i).getKey());
      }

      // 5. clear: delete + deleteRange
      client.deleteAsync(k).join();
      assertFalse(client.getAsync(k).join().isPresent());

      ByteString scanPrefixEnd = Key.toRawKey(scanPrefix).nextPrefix().toByteString();
      client.deleteRangeAsync(scanPrefix, scanPrefixEnd).join();
      List<KvPair> left =
          client
              .scanAsync(scanPrefix, 1000)
              .join()
              .stream()
              .filter(kv -> kv.getKey().toStringUtf8().startsWith("scan-async-"))
              .collect(Collectors.toList());
      assertTrue(left.isEmpty());
    }
  }

  /** Checks whether the local tiup playground PD endpoint (127.0.0.1:2379) is reachable. */
  private static boolean isPlaygroundUp() {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", 2379), 2000);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
