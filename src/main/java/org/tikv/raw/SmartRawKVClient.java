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
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.common.exception.CircuitBreakerOpenException;
import org.tikv.common.util.HistogramUtils;
import org.tikv.common.util.ScanOption;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.service.failsafe.CircuitBreaker;

public class SmartRawKVClient implements RawKVClientBase {
  private static final Logger logger = LoggerFactory.getLogger(SmartRawKVClient.class);

  private static final Histogram REQUEST_LATENCY =
      HistogramUtils.buildDuration()
          .name("client_java_smart_raw_requests_latency")
          .help("client smart raw request latency.")
          .labelNames("type", "cluster")
          .register();

  private static final Counter REQUEST_SUCCESS =
      Counter.build()
          .name("client_java_smart_raw_requests_success")
          .help("client smart raw request success.")
          .labelNames("type", "cluster")
          .register();

  private static final Counter REQUEST_FAILURE =
      Counter.build()
          .name("client_java_smart_raw_requests_failure")
          .help("client smart raw request failure.")
          .labelNames("type", "cluster")
          .register();

  private static final Counter CIRCUIT_BREAKER_OPENED =
      Counter.build()
          .name("client_java_smart_raw_circuit_breaker_opened")
          .help("client smart raw circuit breaker opened.")
          .labelNames("type", "cluster")
          .register();

  private final RawKVClientBase client;
  private final CircuitBreaker circuitBreaker;

  public SmartRawKVClient(RawKVClientBase client, CircuitBreaker breaker) {
    this.client = client;
    this.circuitBreaker = breaker;
  }

  @Override
  public void put(ByteString key, ByteString value) {
    callWithCircuitBreaker("put", () -> client.put(key, value));
  }

  @Override
  public void put(ByteString key, ByteString value, long ttl) {
    callWithCircuitBreaker("put", () -> client.put(key, value, ttl));
  }

  @Override
  public void putAtomic(ByteString key, ByteString value, long ttl) {
    callWithCircuitBreaker("putAtomic", () -> client.putAtomic(key, value, ttl));
  }

  @Override
  public ByteString putIfAbsent(ByteString key, ByteString value) {
    return callWithCircuitBreaker("putIfAbsent", () -> client.putIfAbsent(key, value));
  }

  @Override
  public ByteString putIfAbsent(ByteString key, ByteString value, long ttl) {
    return callWithCircuitBreaker("putIfAbsent", () -> client.putIfAbsent(key, value, ttl));
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs) {
    callWithCircuitBreaker("batchPut", () -> client.batchPut(kvPairs));
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs, long ttl) {
    callWithCircuitBreaker("batchPut", () -> client.batchPut(kvPairs, ttl));
  }

  @Override
  public void batchPutAtomic(Map<ByteString, ByteString> kvPairs) {
    callWithCircuitBreaker("batchPutAtomic", () -> client.batchPutAtomic(kvPairs));
  }

  @Override
  public void batchPutAtomic(Map<ByteString, ByteString> kvPairs, long ttl) {
    callWithCircuitBreaker("batchPutAtomic", () -> client.batchPutAtomic(kvPairs, ttl));
  }

  @Override
  public ByteString get(ByteString key) {
    return callWithCircuitBreaker("get", () -> client.get(key));
  }

  @Override
  public List<Kvrpcpb.KvPair> batchGet(List<ByteString> keys) {
    return callWithCircuitBreaker("batchGet", () -> client.batchGet(keys));
  }

  @Override
  public void batchDelete(List<ByteString> keys) {
    callWithCircuitBreaker("batchDelete", () -> client.batchDelete(keys));
  }

  @Override
  public void batchDeleteAtomic(List<ByteString> keys) {
    callWithCircuitBreaker("batchDeleteAtomic", () -> client.batchDeleteAtomic(keys));
  }

  @Override
  public Long getKeyTTL(ByteString key) {
    return callWithCircuitBreaker("getKeyTTL", () -> client.getKeyTTL(key));
  }

  @Override
  public List<List<Kvrpcpb.KvPair>> batchScan(List<ScanOption> ranges) {
    return callWithCircuitBreaker("batchScan", () -> client.batchScan(ranges));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, int limit) {
    return callWithCircuitBreaker("scan", () -> client.scan(startKey, endKey, limit));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(
      ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    return callWithCircuitBreaker("scan", () -> client.scan(startKey, endKey, limit, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit) {
    return callWithCircuitBreaker("scan", () -> client.scan(startKey, limit));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit, boolean keyOnly) {
    return callWithCircuitBreaker("scan", () -> client.scan(startKey, limit, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey) {
    return callWithCircuitBreaker("scan", () -> client.scan(startKey, endKey));
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly) {
    return callWithCircuitBreaker("scan", () -> client.scan(startKey, endKey, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, int limit, boolean keyOnly) {
    return callWithCircuitBreaker("scanPrefix", () -> client.scanPrefix(prefixKey, limit, keyOnly));
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey) {
    return callWithCircuitBreaker("scanPrefix", () -> client.scanPrefix(prefixKey));
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly) {
    return callWithCircuitBreaker("scanPrefix", () -> client.scanPrefix(prefixKey, keyOnly));
  }

  @Override
  public void delete(ByteString key) {
    callWithCircuitBreaker("delete", () -> client.delete(key));
  }

  @Override
  public void deleteAtomic(ByteString key) {
    callWithCircuitBreaker("deleteAtomic", () -> client.deleteAtomic(key));
  }

  @Override
  public void deleteRange(ByteString startKey, ByteString endKey) {
    callWithCircuitBreaker("deleteRange", () -> client.deleteRange(startKey, endKey));
  }

  @Override
  public void deletePrefix(ByteString key) {
    callWithCircuitBreaker("deletePrefix", () -> client.deletePrefix(key));
  }

  @Override
  public TiSession getSession() {
    return client.getSession();
  }

  <T> T callWithCircuitBreaker(String funcName, Function1<T> func) {
    String[] labels = new String[] {funcName, getSession().getPDClient().getClusterId().toString()};

    Histogram.Timer requestTimer = REQUEST_LATENCY.labels(labels).startTimer();
    try {
      T result = callWithCircuitBreaker0(funcName, func);
      REQUEST_SUCCESS.labels(labels).inc();
      return result;
    } catch (Exception e) {
      REQUEST_FAILURE.labels(labels).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
    }
  }

  private <T> T callWithCircuitBreaker0(String funcName, Function1<T> func) {
    if (circuitBreaker.allowRequest()) {
      try {
        T result = func.apply();
        circuitBreaker.getMetrics().recordSuccess();
        return result;
      } catch (Exception e) {
        circuitBreaker.getMetrics().recordFailure();
        throw e;
      }
    } else if (circuitBreaker.attemptExecution()) {
      logger.debug("attemptExecution");
      try {
        T result = func.apply();
        circuitBreaker.getMetrics().recordSuccess();
        circuitBreaker.recordAttemptSuccess();
        logger.debug("markSuccess");
        return result;
      } catch (Exception e) {
        circuitBreaker.getMetrics().recordFailure();
        circuitBreaker.recordAttemptFailure();
        logger.debug("markNonSuccess");
        throw e;
      }
    } else {
      logger.debug("Circuit Breaker Opened");
      CIRCUIT_BREAKER_OPENED
          .labels(funcName, getSession().getPDClient().getClusterId().toString())
          .inc();
      throw new CircuitBreakerOpenException();
    }
  }

  private void callWithCircuitBreaker(String funcName, Function0 func) {
    callWithCircuitBreaker(
        funcName,
        (Function1<Void>)
            () -> {
              func.apply();
              return null;
            });
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  public interface Function1<T> {
    T apply();
  }

  public interface Function0 {
    void apply();
  }
}
