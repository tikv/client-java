/*
 * Copyright 2021 TiKV Project Authors.
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

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.tikv.common.TiSession;
import org.tikv.common.util.Pair;
import org.tikv.common.util.ScanOption;
import org.tikv.kvproto.Kvrpcpb;

public interface RawKVClientBase extends AutoCloseable {

  // https://www.github.com/pingcap/tidb/blob/master/store/tikv/rawkv.go
  int MAX_RAW_SCAN_LIMIT = 10240;
  int MAX_RAW_BATCH_LIMIT = 1024;
  int RAW_BATCH_PUT_SIZE = 1024 * 1024;
  int RAW_BATCH_GET_SIZE = 16 * 1024;
  int RAW_BATCH_DELETE_SIZE = 16 * 1024;

  /**
   * Put a raw key-value pair to TiKV
   *
   * @param key raw key
   * @param value raw value
   */
  void put(ByteString key, ByteString value);

  /**
   * Put a raw key-value pair to TiKV
   *
   * @param key raw key
   * @param value raw value
   * @param ttl the ttl of the key (in seconds), 0 means the key will never be outdated
   */
  void put(ByteString key, ByteString value, long ttl);

  /**
   * Put a raw key-value pair to TiKV
   *
   * @see #put(ByteString, ByteString, long)
   * @param key raw key
   * @param value raw value
   * @param duration the duration of the key, 0 means the key will never be outdated
   * @param timeUnit the time unit of duration
   */
  default void put(ByteString key, ByteString value, long duration, TimeUnit timeUnit) {
    Long seconds = timeUnit.toSeconds(duration);
    put(key, value, seconds);
  }

  /**
   * Put a key-value pair if it does not exist. This API is atomic.
   *
   * <p>To use this API, please enable `tikv.enable_atomic_for_cas`.
   *
   * @param key key
   * @param value value
   * @return a ByteString. returns Optional.EMPTY if the value is written successfully. returns the
   *     previous key if the value already exists, and does not write to TiKV.
   */
  Optional<ByteString> putIfAbsent(ByteString key, ByteString value);

  /**
   * Put a key-value pair with TTL if it does not exist. This API is atomic.
   *
   * <p>To use this API, please enable `tikv.enable_atomic_for_cas`.
   *
   * @param key key
   * @param value value
   * @param ttl TTL of key (in seconds), 0 means the key will never be outdated.
   * @return a ByteString. returns Optional.EMPTY if the value is written successfully. returns the
   *     previous key if the value already exists, and does not write to TiKV.
   */
  Optional<ByteString> putIfAbsent(ByteString key, ByteString value, long ttl);

  /**
   * Put a key-value pair with TTL if it does not exist. This API is atomic.
   *
   * <p>To use this API, please enable `tikv.enable_atomic_for_cas`.
   *
   * @see #putIfAbsent(ByteString, ByteString, long)
   * @param key key
   * @param value value
   * @param duration duration of key, 0 means the key will never be outdated.
   * @param timeUnit the time unit of duration
   * @return a ByteString. returns Optional.EMPTY if the value is written successfully. returns the
   *     previous key if the value already exists, and does not write to TiKV.
   */
  default Optional<ByteString> putIfAbsent(
      ByteString key, ByteString value, long duration, TimeUnit timeUnit) {
    Long seconds = timeUnit.toSeconds(duration);
    return putIfAbsent(key, value, seconds);
  }

  /**
   * Put a key-value pair if the prevValue matched the value in TiKV. This API is atomic.
   *
   * <p>To use this API, please enable `tikv.enable_atomic_for_cas`.
   *
   * @param key key
   * @param value value
   */
  void compareAndSet(ByteString key, Optional<ByteString> prevValue, ByteString value);

  /**
   * pair if the prevValue matched the value in TiKV. This API is atomic.
   *
   * <p>To use this API, please enable `tikv.enable_atomic_for_cas`.
   *
   * @param key key
   * @param value value
   * @param ttl TTL of key (in seconds), 0 means the key will never be outdated.
   */
  void compareAndSet(ByteString key, Optional<ByteString> prevValue, ByteString value, long ttl);

  /**
   * pair if the prevValue matched the value in TiKV. This API is atomic.
   *
   * <p>To use this API, please enable `tikv.enable_atomic_for_cas`.
   *
   * @see #compareAndSet(ByteString, Optional, ByteString, long)
   * @param key key
   * @param value value
   * @param duration duration of key , 0 means the key will never be outdated.
   * @param timeUnit time unit of duration
   */
  default void compareAndSet(
      ByteString key,
      Optional<ByteString> prevValue,
      ByteString value,
      long duration,
      TimeUnit timeUnit) {
    long seconds = timeUnit.toSeconds(duration);
    compareAndSet(key, prevValue, value, seconds);
  }

  /**
   * Put a set of raw key-value pair to TiKV.
   *
   * @param kvPairs kvPairs
   */
  void batchPut(Map<ByteString, ByteString> kvPairs);

  /**
   * Put a set of raw key-value pair to TiKV.
   *
   * @param kvPairs kvPairs
   * @param ttl the TTL of keys to be put (in seconds), 0 means the keys will never be outdated
   */
  void batchPut(Map<ByteString, ByteString> kvPairs, long ttl);

  /**
   * Put a set of raw key-value pair to TiKV.
   *
   * @see #batchPut(Map, long)
   * @param kvPairs kvPairs
   * @param duration the duration of keys to be put, 0 means the keys will never be outdated
   * @param timeUnit time unit of duration
   */
  default void batchPut(Map<ByteString, ByteString> kvPairs, long duration, TimeUnit timeUnit) {
    long seconds = timeUnit.toSeconds(duration);
    batchPut(kvPairs, seconds);
  }

  /**
   * Get a raw key-value pair from TiKV if key exists
   *
   * @param key raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  Optional<ByteString> get(ByteString key);

  /**
   * Get a list of raw key-value pair from TiKV if key exists
   *
   * @param keys list of raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  List<Kvrpcpb.KvPair> batchGet(List<ByteString> keys);

  /**
   * Delete a list of raw key-value pair from TiKV if key exists
   *
   * @param keys list of raw key
   */
  void batchDelete(List<ByteString> keys);

  /**
   * Get the TTL of a raw key from TiKV if key exists
   *
   * @param key raw key
   * @return a Long indicating the TTL of key ttl is a non-null long value indicating TTL if key
   *     exists. - ttl=0 if the key will never be outdated. - ttl=null if the key does not exist
   */
  Optional<Long> getKeyTTL(ByteString key);

  /**
   * Create a new `batch scan` request with `keyOnly` option Once resolved this request will result
   * in a set of scanners over the given keys.
   *
   * <p>WARNING: This method is experimental. The `each_limit` parameter does not work as expected.
   * It does not limit the number of results returned of each range, instead it limits the number of
   * results in each region of each range. As a result, you may get more than each_limit key-value
   * pairs for each range. But you should not miss any entries.
   *
   * @param ranges a list of ranges
   * @return a set of scanners for keys over the given keys.
   */
  List<List<ByteString>> batchScanKeys(List<Pair<ByteString, ByteString>> ranges, int eachLimit);

  /**
   * Create a new `batch scan` request. Once resolved this request will result in a set of scanners
   * over the given keys.
   *
   * <p>WARNING: This method is experimental. The `each_limit` parameter does not work as expected.
   * It does not limit the number of results returned of each range, instead it limits the number of
   * results in each region of each range. As a result, you may get more than each_limit key-value
   * pairs for each range. But you should not miss any entries.
   *
   * @param ranges a list of `ScanOption` for each range
   * @return a set of scanners over the given keys.
   */
  List<List<Kvrpcpb.KvPair>> batchScan(List<ScanOption> ranges);

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return list of key-value pairs in range
   */
  List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, int limit);

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return list of key-value pairs in range
   */
  List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, int limit, boolean keyOnly);

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return list of key-value pairs in range
   */
  List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit);

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return list of key-value pairs in range
   */
  List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit, boolean keyOnly);

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return list of key-value pairs in range
   */
  List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey);

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param keyOnly whether to scan in key-only mode
   * @return list of key-value pairs in range
   */
  List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly);

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param limit limit of keys retrieved
   * @param keyOnly whether to scan in keyOnly mode
   * @return kvPairs with the specified prefix
   */
  List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, int limit, boolean keyOnly);

  List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey);

  List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly);

  /**
   * Delete a raw key-value pair from TiKV if key exists
   *
   * @param key raw key to be deleted
   */
  void delete(ByteString key);

  /**
   * Delete all raw key-value pairs in range [startKey, endKey) from TiKV
   *
   * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
   * range along with deleteRange API, the result will be undefined.
   *
   * @param startKey raw start key to be deleted
   * @param endKey raw start key to be deleted
   */
  void deleteRange(ByteString startKey, ByteString endKey);

  /**
   * Delete all raw key-value pairs with the prefix `key` from TiKV
   *
   * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
   * range along with deleteRange API, the result will be undefined.
   *
   * @param key prefix of keys to be deleted
   */
  void deletePrefix(ByteString key);

  /** Get the session of the current client */
  TiSession getSession();
}
