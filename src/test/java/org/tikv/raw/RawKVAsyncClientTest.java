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
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.MockServerTest;
import org.tikv.common.exception.RegionException;
import org.tikv.common.key.Key;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;

/** Unit tests for RawKVAsyncClient based on KVMockServer, without a real TiKV cluster. */
public class RawKVAsyncClientTest extends MockServerTest {

  private RawKVAsyncClient client;

  private static final ByteString K1 = ByteString.copyFromUtf8("k1");
  private static final ByteString K2 = ByteString.copyFromUtf8("k2");
  private static final ByteString V1 = ByteString.copyFromUtf8("v1");
  private static final ByteString V2 = ByteString.copyFromUtf8("v2");
  private static final ByteString V3 = ByteString.copyFromUtf8("v3");

  @Before
  public void initClient() {
    session.getConf().setEnableAtomicForCAS(true);
    client = session.createRawAsyncClient();
    server.clearAllMap();
  }

  @After
  public void closeClient() {
    client.close();
  }

  private ByteString key(String s) {
    return ByteString.copyFromUtf8(s);
  }

  @Test
  public void testPutGetDeleteAsync() {
    client.putAsync(K1, V1).join();
    assertEquals(Optional.of(V1), client.getAsync(K1).join());

    client.putAsync(K1, V2).join();
    assertEquals(Optional.of(V2), client.getAsync(K1).join());

    client.deleteAsync(K1).join();
    assertEquals(Optional.empty(), client.getAsync(K1).join());

    // Non-existent key
    assertEquals(Optional.empty(), client.getAsync(K2).join());
  }

  @Test
  public void testPutIfAbsentAsync() {
    // First put succeeds
    assertEquals(Optional.empty(), client.putIfAbsentAsync(K1, V1).join());
    assertEquals(Optional.of(V1), client.getAsync(K1).join());

    // Key exists: return the old value without overwriting
    assertEquals(Optional.of(V1), client.putIfAbsentAsync(K1, V2).join());
    assertEquals(Optional.of(V1), client.getAsync(K1).join());
  }

  @Test
  public void testCompareAndSetAsync() {
    client.putAsync(K1, V1).join();

    // prev mismatch: first=false, second carries the current value
    Pair<Boolean, Optional<ByteString>> result =
        client.compareAndSetAsync(K1, Optional.of(V2), V3).join();
    assertFalse(result.first);
    assertEquals(Optional.of(V1), result.second);
    assertEquals(Optional.of(V1), client.getAsync(K1).join());

    // prev matches: first=true, second is meaningless (Optional.empty)
    result = client.compareAndSetAsync(K1, Optional.of(V1), V3).join();
    assertTrue(result.first);
    assertEquals(Optional.empty(), result.second);
    assertEquals(Optional.of(V3), client.getAsync(K1).join());

    // key does not exist: first=false, second=Optional.empty() (no ambiguity with an empty value)
    Pair<Boolean, Optional<ByteString>> notExist =
        client.compareAndSetAsync(K2, Optional.of(V2), V3).join();
    assertFalse(notExist.first);
    assertEquals(Optional.empty(), notExist.second);
  }

  @Test
  public void testScanAsync() {
    int n = 50;
    for (int i = 0; i < n; i++) {
      client.putAsync(key("scan-" + String.format("%03d", i)), key("value-" + i)).join();
    }

    // Full scan, ordered
    List<KvPair> all = client.scanAsync(key("scan-"), 100).join();
    assertEquals(n, all.size());
    for (int i = 0; i < n; i++) {
      assertEquals(key("scan-" + String.format("%03d", i)), all.get(i).getKey());
    }

    // limit takes effect
    List<KvPair> first10 = client.scanAsync(key("scan-"), 10).join();
    assertEquals(10, first10.size());

    // keyOnly: value is empty
    List<KvPair> keyOnly = client.scanAsync(key("scan-"), 100, true).join();
    assertEquals(n, keyOnly.size());
    assertTrue(keyOnly.get(0).getValue().isEmpty());
  }

  @Test
  public void testDeleteRangeAsync() {
    int n = 50;
    ByteString prefix = key("range-");
    for (int i = 0; i < n; i++) {
      client.putAsync(key("range-" + String.format("%03d", i)), V1).join();
    }
    assertEquals(n, client.scanAsync(prefix, 100).join().size());

    ByteString endKey = Key.toRawKey(prefix).nextPrefix().toByteString();
    client.deleteRangeAsync(prefix, endKey).join();
    assertEquals(0, client.scanAsync(prefix, 100).join().size());
  }

  @Test
  public void testGetKeyTTLAsync() {
    client.putAsync(K1, V1, 100).join();

    Optional<Long> ttl = client.getKeyTTLAsync(K1).join();
    assertTrue(ttl.isPresent());
    // The mock does not simulate TTL countdown; return 0 to indicate no expiration
    assertEquals(Long.valueOf(0), ttl.get());

    assertFalse(client.getKeyTTLAsync(K2).join().isPresent());
  }

  @Test
  public void testRegionErrorNoRetryAsync() throws Exception {
    // Inject an EpochNotMatch RegionError: the client performs no retry, the future fails
    // immediately with RegionException so that the caller can decide what to do.
    server.put(K1, V1);
    server.putError(
        "k1",
        () ->
            Errorpb.Error.newBuilder()
                .setEpochNotMatch(
                    Errorpb.EpochNotMatch.newBuilder()
                        .addCurrentRegions(region.getMeta())
                        .build()));

    try {
      client.getAsync(K1).get(10, TimeUnit.SECONDS);
      fail("should throw RegionException without retry");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof RegionException);
      RegionException regionException = (RegionException) e.getCause();
      assertTrue(regionException.getRegionErr().hasEpochNotMatch());
    }
  }

  @Test
  public void testCustomMetadataExecutorAsync() throws Exception {
    ExecutorService customExecutor = Executors.newFixedThreadPool(2);
    RawKVAsyncClient customClient =
        new RawKVAsyncClient(session, session.getRegionStoreClientBuilder(), customExecutor);
    try {
      // Cache miss on first use forces the lookup through the custom executor.
      customClient.putAsync(K1, V1).join();
      assertEquals(Optional.of(V1), customClient.getAsync(K1).join());
    } finally {
      customClient.close();
    }

    // close() must not shut down a caller-supplied executor: it stays usable afterwards.
    assertFalse(customExecutor.isShutdown());
    assertEquals((Integer) 42, customExecutor.submit(() -> 42).get(5, TimeUnit.SECONDS));
    customExecutor.shutdown();
  }

  @Test
  public void testRejectedMetadataExecutorAsync() throws Exception {
    // A shut-down executor rejects slow-path metadata lookups: the failure must be captured
    // in the returned future instead of being thrown synchronously.
    ExecutorService closedExecutor = Executors.newSingleThreadExecutor();
    closedExecutor.shutdown();
    RawKVAsyncClient rejectedClient =
        new RawKVAsyncClient(session, session.getRegionStoreClientBuilder(), closedExecutor);
    try {
      // Clear the region cache to force the slow path through the executor.
      session.getRegionManager().invalidateAll();
      CompletableFuture<Optional<ByteString>> future = rejectedClient.getAsync(K2);
      try {
        future.get(10, TimeUnit.SECONDS);
        fail("should fail with RejectedExecutionException");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() instanceof RejectedExecutionException);
      }
    } finally {
      rejectedClient.close();
    }
  }
}
