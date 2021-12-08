/*
 * Copyright 2021 PingCAP, Inc.
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

package org.tikv.raw;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.CircuitBreakerOpenException;
import org.tikv.common.util.Pair;
import org.tikv.common.util.ScanOption;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.service.failsafe.CircuitBreaker;
import org.tikv.service.failsafe.CircuitBreakerImpl;
import org.tikv.service.failsafe.CircuitBreakerMetrics;

public class SmartRawKVClient implements RawKVClientBase {
  private static final Logger logger = LoggerFactory.getLogger(SmartRawKVClient.class);

  private final RawKVClientBase client;
  private final CircuitBreaker circuitBreaker;
  private final CircuitBreakerMetrics circuitBreakerMetrics;

  public SmartRawKVClient(RawKVClientBase client, TiConfiguration conf) {
    this.client = client;
    this.circuitBreaker = new CircuitBreakerImpl(conf);
    this.circuitBreakerMetrics = this.circuitBreaker.getMetrics();
  }

  @Override
  public void put(ByteString key, ByteString value) {
    callWithCircuitBreaker(() -> client.put(key, value));
  }

  @Override
  public void put(ByteString key, ByteString value, long ttl) {
    callWithCircuitBreaker(() -> client.put(key, value, ttl));
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value) {
    return callWithCircuitBreaker(() -> client.putIfAbsent(key, value));
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value, long ttl) {
    return callWithCircuitBreaker(() -> client.putIfAbsent(key, value, ttl));
  }

  @Override
  public void compareAndSet(ByteString key, Optional<ByteString> prevValue, ByteString value) {
    callWithCircuitBreaker(() -> client.compareAndSet(key, prevValue, value));
  }

  @Override
  public void compareAndSet(
      ByteString key, Optional<ByteString> prevValue, ByteString value, long ttl) {
    callWithCircuitBreaker(() -> client.compareAndSet(key, prevValue, value, ttl));
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs) {
    callWithCircuitBreaker(() -> client.batchPut(kvPairs));
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs, long ttl) {
    callWithCircuitBreaker(() -> client.batchPut(kvPairs, ttl));
  }

  @Override
  public Optional<ByteString> get(ByteString key) {
    return callWithCircuitBreaker(() -> client.get(key));
  }

  @Override
  public List<Kvrpcpb.KvPair> batchGet(List<ByteString> keys) {
    return callWithCircuitBreaker(() -> client.batchGet(keys));
  }

  @Override
  public void batchDelete(List<ByteString> keys) {
    callWithCircuitBreaker(() -> client.batchDelete(keys));
  }

  @Override
  public Optional<Long> getKeyTTL(ByteString key) {
    return callWithCircuitBreaker(() -> client.getKeyTTL(key));
  }

  @Override
  public List<List<ByteString>> batchScanKeys(
      List<Pair<ByteString, ByteString>> ranges, int eachLimit) {
    return callWithCircuitBreaker(() -> client.batchScanKeys(ranges, eachLimit));
  }

  @Override
  public List<List<Kvrpcpb.KvPair>> batchScan(List<ScanOption> ranges) {
    return callWithCircuitBreaker(() -> client.batchScan(ranges));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, int limit) {
    return callWithCircuitBreaker(() -> client.scan(startKey, endKey, limit));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(
      ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    return callWithCircuitBreaker(() -> client.scan(startKey, endKey, limit, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit) {
    return callWithCircuitBreaker(() -> client.scan(startKey, limit));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit, boolean keyOnly) {
    return callWithCircuitBreaker(() -> client.scan(startKey, limit, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey) {
    return callWithCircuitBreaker(() -> client.scan(startKey, endKey));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly) {
    return callWithCircuitBreaker(() -> client.scan(startKey, endKey, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, int limit, boolean keyOnly) {
    return callWithCircuitBreaker(() -> client.scanPrefix(prefixKey, limit, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey) {
    return callWithCircuitBreaker(() -> client.scanPrefix(prefixKey));
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly) {
    return callWithCircuitBreaker(() -> client.scanPrefix(prefixKey, keyOnly));
  }

  @Override
  public void delete(ByteString key) {
    callWithCircuitBreaker(() -> client.delete(key));
  }

  @Override
  public void deleteRange(ByteString startKey, ByteString endKey) {
    callWithCircuitBreaker(() -> client.deleteRange(startKey, endKey));
  }

  @Override
  public void deletePrefix(ByteString key) {
    callWithCircuitBreaker(() -> client.deletePrefix(key));
  }

  <T> T callWithCircuitBreaker(Function1<T> func) {
    if (circuitBreaker.allowRequest()) {
      try {
        T result = func.apply();
        circuitBreakerMetrics.recordSuccess();
        return result;
      } catch (Exception e) {
        circuitBreakerMetrics.recordFailure();
        throw e;
      }
    } else if (circuitBreaker.attemptExecution()) {
      logger.info("attemptExecution");
      try {
        T result = func.apply();
        circuitBreakerMetrics.recordSuccess();
        circuitBreaker.recordAttemptSuccess();
        logger.info("markSuccess");
        return result;
      } catch (Exception e) {
        circuitBreakerMetrics.recordFailure();
        circuitBreaker.recordAttemptFailure();
        logger.info("markNonSuccess");
        throw e;
      }
    } else {
      logger.warn("Circuit Breaker Opened");
      throw new CircuitBreakerOpenException();
    }
  }

  void callWithCircuitBreaker(Function0 func) {
    callWithCircuitBreaker(
        (Function1<Void>)
            () -> {
              func.apply();
              return null;
            });
  }

  @Override
  public void close() throws Exception {
    Exception err = null;
    try {
      client.close();
    } catch (Exception e) {
      err = e;
    }

    try {
      circuitBreaker.close();
    } catch (Exception e) {
      err = e;
    }

    if (err != null) {
      throw err;
    }
  }

  public interface Function1<T> {
    T apply();
  }

  public interface Function0 {
    void apply();
  }
}
