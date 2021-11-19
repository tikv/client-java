/*
 * Copyright 2018 PingCAP, Inc.
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

import static org.tikv.common.util.ClientUtils.*;

import com.google.protobuf.ByteString;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.log.SlowLog;
import org.tikv.common.log.SlowLogEmptyImpl;
import org.tikv.common.log.SlowLogImpl;
import org.tikv.common.operation.iterator.RawScanIterator;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.*;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class RawKVClient implements AutoCloseable {
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final ExecutorService batchGetThreadPool;
  private final ExecutorService batchPutThreadPool;
  private final ExecutorService batchDeleteThreadPool;
  private final ExecutorService batchScanThreadPool;
  private final ExecutorService deleteRangeThreadPool;
  private static final Logger logger = LoggerFactory.getLogger(RawKVClient.class);

  // https://www.github.com/pingcap/tidb/blob/master/store/tikv/rawkv.go
  private static final int MAX_RAW_SCAN_LIMIT = 10240;
  private static final int MAX_RAW_BATCH_LIMIT = 1024;
  private static final int RAW_BATCH_PUT_SIZE = 1024 * 1024; // 1 MB
  private static final int RAW_BATCH_GET_SIZE = 16 * 1024; // 16 K
  private static final int RAW_BATCH_DELETE_SIZE = 16 * 1024; // 16 K
  private static final int RAW_BATCH_SCAN_SIZE = 16;
  private static final int RAW_BATCH_PAIR_COUNT = 512;

  public static final Histogram RAW_REQUEST_LATENCY =
      Histogram.build()
          .name("client_java_raw_requests_latency")
          .help("client raw request latency.")
          .labelNames("type")
          .register();

  public static final Counter RAW_REQUEST_SUCCESS =
      Counter.build()
          .name("client_java_raw_requests_success")
          .help("client raw request success.")
          .labelNames("type")
          .register();

  public static final Counter RAW_REQUEST_FAILURE =
      Counter.build()
          .name("client_java_raw_requests_failure")
          .help("client raw request failure.")
          .labelNames("type")
          .register();

  private static final TiKVException ERR_MAX_SCAN_LIMIT_EXCEEDED =
      new TiKVException("limit should be less than MAX_RAW_SCAN_LIMIT");

  public RawKVClient(TiSession session, RegionStoreClientBuilder clientBuilder) {
    Objects.requireNonNull(session, "session is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.conf = session.getConf();
    this.clientBuilder = clientBuilder;
    this.batchGetThreadPool = session.getThreadPoolForBatchGet();
    this.batchPutThreadPool = session.getThreadPoolForBatchPut();
    this.batchDeleteThreadPool = session.getThreadPoolForBatchDelete();
    this.batchScanThreadPool = session.getThreadPoolForBatchScan();
    this.deleteRangeThreadPool = session.getThreadPoolForDeleteRange();
  }

  @Override
  public void close() {}

  /**
   * Put a raw key-value pair to TiKV
   *
   * @param key raw key
   * @param value raw value
   */
  public void put(ByteString key, ByteString value) {
    put(key, value, 0);
  }

  /**
   * Put a raw key-value pair to TiKV
   *
   * @param key raw key
   * @param value raw value
   * @param ttl the ttl of the key (in seconds), 0 means the key will never be outdated
   */
  public void put(ByteString key, ByteString value, long ttl) {
    put(key, value, ttl, false);
  }

  /**
   * Put a raw key-value pair to TiKV. This API is atomic.
   *
   * @param key raw key
   * @param value raw value
   * @param ttl the ttl of the key (in seconds), 0 means the key will never be outdated
   */
  public void putAtomic(ByteString key, ByteString value, long ttl) {
    put(key, value, ttl, true);
  }

  private void put(ByteString key, ByteString value, long ttl, boolean atomic) {
    String label = "client_raw_put";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVWriteSlowLogInMS(),
            new HashMap<String, String>(2) {
              {
                put("func", "put");
                put("key", KeyUtils.formatBytesUTF8(key));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog);
    try {
      while (true) {
        RegionStoreClient client = clientBuilder.build(key, backOffer);
        slowLog.addProperty("region", client.getRegion().toString());
        try {
          client.rawPut(backOffer, key, value, ttl, atomic);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for put error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Put a key-value pair if it does not exist. This API is atomic.
   *
   * @param key key
   * @param value value
   * @return a ByteString. returns ByteString.EMPTY if the value is written successfully. returns
   *     the previous key if the value already exists, and does not write to TiKV.
   */
  public ByteString putIfAbsent(ByteString key, ByteString value) {
    return putIfAbsent(key, value, 0L);
  }

  /**
   * Put a key-value pair with TTL if it does not exist. This API is atomic.
   *
   * @param key key
   * @param value value
   * @param ttl TTL of key (in seconds), 0 means the key will never be outdated.
   * @return a ByteString. returns ByteString.EMPTY if the value is written successfully. returns
   *     the previous key if the value already exists, and does not write to TiKV.
   */
  public ByteString putIfAbsent(ByteString key, ByteString value, long ttl) {
    String label = "client_raw_put_if_absent";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVWriteSlowLogInMS(),
            new HashMap<String, String>(2) {
              {
                put("func", "putIfAbsent");
                put("key", KeyUtils.formatBytesUTF8(key));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog);
    try {
      while (true) {
        RegionStoreClient client = clientBuilder.build(key, backOffer);
        slowLog.addProperty("region", client.getRegion().toString());
        try {
          ByteString result = client.rawPutIfAbsent(backOffer, key, value, ttl);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return result;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for putIfAbsent error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Put a set of raw key-value pair to TiKV, this API does not ensure the operation is atomic.
   *
   * @param kvPairs kvPairs
   */
  public void batchPut(Map<ByteString, ByteString> kvPairs) {
    batchPut(kvPairs, 0);
  }

  /**
   * Put a set of raw key-value pair to TiKV, this API does not ensure the operation is atomic.
   *
   * @param kvPairs kvPairs
   * @param ttl the TTL of keys to be put (in seconds), 0 means the keys will never be outdated
   */
  public void batchPut(Map<ByteString, ByteString> kvPairs, long ttl) {
    batchPut(kvPairs, ttl, false);
  }

  /**
   * Put a set of raw key-value pair to TiKV, this API is atomic
   *
   * @param kvPairs kvPairs
   */
  public void batchPutAtomic(Map<ByteString, ByteString> kvPairs) {
    batchPutAtomic(kvPairs, 0);
  }

  /**
   * Put a set of raw key-value pair to TiKV, this API is atomic.
   *
   * @param kvPairs kvPairs
   * @param ttl the TTL of keys to be put (in seconds), 0 means the keys will never be outdated
   */
  public void batchPutAtomic(Map<ByteString, ByteString> kvPairs, long ttl) {
    batchPut(kvPairs, ttl, true);
  }

  private void batchPut(Map<ByteString, ByteString> kvPairs, long ttl, boolean atomic) {
    String label = "client_raw_batch_put";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVBatchWriteSlowLogInMS(),
            new HashMap<String, String>(2) {
              {
                put("func", "batchPut");
                put("keySize", String.valueOf(kvPairs.size()));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVBatchWriteTimeoutInMS(), slowLog);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchWriteTimeoutInMS();
      doSendBatchPut(backOffer, kvPairs, ttl, atomic, deadline);
      RAW_REQUEST_SUCCESS.labels(label).inc();
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Get a raw key-value pair from TiKV if key exists
   *
   * @param key raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString get(ByteString key) {
    String label = "client_raw_get";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVReadSlowLogInMS(),
            new HashMap<String, String>(2) {
              {
                put("func", "get");
                put("key", KeyUtils.formatBytesUTF8(key));
              }
            });

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVReadTimeoutInMS(), slowLog);
    try {
      while (true) {
        RegionStoreClient client = clientBuilder.build(key, backOffer);
        slowLog.addProperty("region", client.getRegion().toString());
        try {
          ByteString result = client.rawGet(backOffer, key);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return result;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for get error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Get a list of raw key-value pair from TiKV if key exists
   *
   * @param keys list of raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public List<KvPair> batchGet(List<ByteString> keys) {
    String label = "client_raw_batch_get";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVBatchReadSlowLogInMS(),
            new HashMap<String, String>(2) {
              {
                put("func", "batchGet");
                put("keySize", String.valueOf(keys.size()));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVBatchReadTimeoutInMS(), slowLog);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchReadTimeoutInMS();
      List<KvPair> result = doSendBatchGet(backOffer, keys, deadline);
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Delete a list of raw key-value pair from TiKV if key exists
   *
   * @param keys list of raw key
   */
  public void batchDelete(List<ByteString> keys) {
    batchDelete(keys, false);
  }

  /**
   * Delete a list of raw key-value pair from TiKV if key exists, this API is atomic
   *
   * @param keys list of raw key
   */
  public void batchDeleteAtomic(List<ByteString> keys) {
    batchDelete(keys, true);
  }

  private void batchDelete(List<ByteString> keys, boolean atomic) {
    String label = "client_raw_batch_delete";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVBatchWriteSlowLogInMS(),
            new HashMap<String, String>(2) {
              {
                put("func", "batchDelete");
                put("keySize", String.valueOf(keys.size()));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVBatchWriteTimeoutInMS(), slowLog);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchWriteTimeoutInMS();
      doSendBatchDelete(backOffer, keys, atomic, deadline);
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Get the TTL of a raw key from TiKV if key exists
   *
   * @param key raw key
   * @return a Long indicating the TTL of key ttl is a non-null long value indicating TTL if key
   *     exists. - ttl=0 if the key will never be outdated. - ttl=null if the key does not exist
   */
  public Long getKeyTTL(ByteString key) {
    String label = "client_raw_get_key_ttl";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVReadSlowLogInMS(),
            new HashMap<String, String>(2) {
              {
                put("func", "getKeyTTL");
                put("key", KeyUtils.formatBytesUTF8(key));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVReadTimeoutInMS(), slowLog);
    try {
      while (true) {
        RegionStoreClient client = clientBuilder.build(key, backOffer);
        slowLog.addProperty("region", client.getRegion().toString());
        try {
          Long result = client.rawGetKeyTTL(backOffer, key);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return result;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for getKeyTTL error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  public List<List<KvPair>> batchScan(List<ScanOption> ranges) {
    String label = "client_raw_batch_scan";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    long deadline = System.currentTimeMillis() + conf.getRawKVScanTimeoutInMS();
    List<Future<Pair<Integer, List<KvPair>>>> futureList = new ArrayList<>();
    try {
      if (ranges.isEmpty()) {
        return new ArrayList<>();
      }
      ExecutorCompletionService<Pair<Integer, List<KvPair>>> completionService =
          new ExecutorCompletionService<>(batchScanThreadPool);
      int num = 0;
      for (ScanOption scanOption : ranges) {
        int i = num;
        futureList.add(completionService.submit(() -> Pair.create(i, scan(scanOption))));
        ++num;
      }
      List<List<KvPair>> scanResults = new ArrayList<>();
      for (int i = 0; i < num; i++) {
        scanResults.add(new ArrayList<>());
      }
      for (int i = 0; i < num; i++) {
        try {
          Future<Pair<Integer, List<KvPair>>> future =
              completionService.poll(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          if (future == null) {
            throw new TiKVException("TimeOut Exceeded for current operation.");
          }
          Pair<Integer, List<KvPair>> scanResult = future.get();
          scanResults.set(scanResult.first, scanResult.second);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new TiKVException("Current thread interrupted.", e);
        } catch (ExecutionException e) {
          throw new TiKVException("Execution exception met.", e);
        }
      }
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return scanResults;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      for (Future<Pair<Integer, List<KvPair>>> future : futureList) {
        future.cancel(true);
      }
      throw e;
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit) {
    return scan(startKey, endKey, limit, false);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    String label = "client_raw_scan";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVScanSlowLogInMS(),
            new HashMap<String, String>(5) {
              {
                put("func", "scan");
                put("startKey", KeyUtils.formatBytesUTF8(startKey));
                put("endKey", KeyUtils.formatBytesUTF8(endKey));
                put("limit", String.valueOf(limit));
                put("keyOnly", String.valueOf(keyOnly));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVScanTimeoutInMS(), slowLog);
    try {
      Iterator<KvPair> iterator =
          rawScanIterator(conf, clientBuilder, startKey, endKey, limit, keyOnly, backOffer);
      List<KvPair> result = new ArrayList<>();
      iterator.forEachRemaining(result::add);
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, int limit) {
    return scan(startKey, limit, false);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, int limit, boolean keyOnly) {
    return scan(startKey, ByteString.EMPTY, limit, keyOnly);
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, ByteString endKey) {
    return scan(startKey, endKey, false);
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param keyOnly whether to scan in key-only mode
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly) {
    String label = "client_raw_scan_without_limit";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVScanSlowLogInMS(),
            new HashMap<String, String>(4) {
              {
                put("func", "scan");
                put("startKey", KeyUtils.formatBytesUTF8(startKey));
                put("endKey", KeyUtils.formatBytesUTF8(endKey));
                put("keyOnly", String.valueOf(keyOnly));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVScanTimeoutInMS(), slowLog);
    try {
      ByteString newStartKey = startKey;
      List<KvPair> result = new ArrayList<>();
      while (true) {
        Iterator<KvPair> iterator =
            rawScanIterator(
                conf,
                clientBuilder,
                newStartKey,
                endKey,
                conf.getScanBatchSize(),
                keyOnly,
                backOffer);
        if (!iterator.hasNext()) {
          break;
        }
        iterator.forEachRemaining(result::add);
        newStartKey = Key.toRawKey(result.get(result.size() - 1).getKey()).next().toByteString();
      }
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  private List<KvPair> scan(ScanOption scanOption) {
    ByteString startKey = scanOption.getStartKey();
    ByteString endKey = scanOption.getEndKey();
    int limit = scanOption.getLimit();
    boolean keyOnly = scanOption.isKeyOnly();
    return scan(startKey, endKey, limit, keyOnly);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param limit limit of keys retrieved
   * @param keyOnly whether to scan in keyOnly mode
   * @return kvPairs with the specified prefix
   */
  public List<KvPair> scanPrefix(ByteString prefixKey, int limit, boolean keyOnly) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), limit, keyOnly);
  }

  public List<KvPair> scanPrefix(ByteString prefixKey) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString());
  }

  public List<KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), keyOnly);
  }

  /**
   * Delete a raw key-value pair from TiKV if key exists
   *
   * @param key raw key to be deleted
   */
  public void delete(ByteString key) {
    delete(key, false);
  }

  /**
   * Delete a raw key-value pair from TiKV if key exists. This API is atomic.
   *
   * @param key raw key to be deleted
   */
  public void deleteAtomic(ByteString key) {
    delete(key, true);
  }

  private void delete(ByteString key, boolean atomic) {
    String label = "client_raw_delete";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog =
        new SlowLogImpl(
            conf.getRawKVWriteSlowLogInMS(),
            new HashMap<String, String>(3) {
              {
                put("func", "delete");
                put("key", KeyUtils.formatBytesUTF8(key));
                put("atomic", String.valueOf(atomic));
              }
            });
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog);
    try {
      while (true) {
        RegionStoreClient client = clientBuilder.build(key, backOffer);
        slowLog.addProperty("region", client.getRegion().toString());
        try {
          client.rawDelete(backOffer, key, atomic);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for delete error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
      slowLog.log();
    }
  }

  /**
   * Delete all raw key-value pairs in range [startKey, endKey) from TiKV
   *
   * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
   * range along with deleteRange API, the result will be undefined.
   *
   * @param startKey raw start key to be deleted
   * @param endKey raw start key to be deleted
   */
  public synchronized void deleteRange(ByteString startKey, ByteString endKey) {
    String label = "client_raw_delete_range";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVCleanTimeoutInMS(), SlowLogEmptyImpl.INSTANCE);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVCleanTimeoutInMS();
      doSendDeleteRange(backOffer, startKey, endKey, deadline);
      RAW_REQUEST_SUCCESS.labels(label).inc();
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Delete all raw key-value pairs with the prefix `key` from TiKV
   *
   * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
   * range along with deleteRange API, the result will be undefined.
   *
   * @param key prefix of keys to be deleted
   */
  public synchronized void deletePrefix(ByteString key) {
    ByteString endKey = Key.toRawKey(key).nextPrefix().toByteString();
    deleteRange(key, endKey);
  }

  private void doSendBatchPut(
      BackOffer backOffer,
      Map<ByteString, ByteString> kvPairs,
      long ttl,
      boolean atomic,
      long deadline) {
    ExecutorCompletionService<List<Batch>> completionService =
        new ExecutorCompletionService<>(batchPutThreadPool);

    List<Future<List<Batch>>> futureList = new ArrayList<>();

    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(clientBuilder.getRegionManager(), kvPairs.keySet(), backOffer);
    List<Batch> batches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          backOffer,
          batches,
          entry.getKey(),
          entry.getValue(),
          entry.getValue().stream().map(kvPairs::get).collect(Collectors.toList()),
          RAW_BATCH_PUT_SIZE,
          MAX_RAW_BATCH_LIMIT);
    }
    Queue<List<Batch>> taskQueue = new LinkedList<>();
    taskQueue.offer(batches);

    while (!taskQueue.isEmpty()) {
      List<Batch> task = taskQueue.poll();
      for (Batch batch : task) {
        futureList.add(
            completionService.submit(
                () -> doSendBatchPutInBatchesWithRetry(batch.getBackOffer(), batch, ttl, atomic)));
      }

      try {
        getTasks(completionService, taskQueue, task, deadline - System.currentTimeMillis());
      } catch (Exception e) {
        for (Future<List<Batch>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }
  }

  private List<Batch> doSendBatchPutInBatchesWithRetry(
      BackOffer backOffer, Batch batch, long ttl, boolean atomic) {
    try (RegionStoreClient client = clientBuilder.build(batch.getRegion(), backOffer)) {
      client.setTimeout(conf.getRawKVBatchWriteTimeoutInMS());
      client.rawBatchPut(backOffer, batch, ttl, atomic);
      return new ArrayList<>();
    } catch (final TiKVException e) {
      // TODO: any elegant way to re-split the ranges if fails?
      backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      logger.warn("ReSplitting ranges for BatchPutRequest", e);
      // retry
      return doSendBatchPutWithRefetchRegion(backOffer, batch);
    }
  }

  private List<Batch> doSendBatchPutWithRefetchRegion(BackOffer backOffer, Batch batch) {
    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(clientBuilder.getRegionManager(), batch.getKeys(), backOffer);
    List<Batch> retryBatches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          backOffer,
          retryBatches,
          entry.getKey(),
          entry.getValue(),
          entry.getValue().stream().map(batch.getMap()::get).collect(Collectors.toList()),
          RAW_BATCH_PUT_SIZE,
          MAX_RAW_BATCH_LIMIT);
    }

    return retryBatches;
  }

  private List<KvPair> doSendBatchGet(BackOffer backOffer, List<ByteString> keys, long deadline) {
    ExecutorCompletionService<Pair<List<Batch>, List<KvPair>>> completionService =
        new ExecutorCompletionService<>(batchGetThreadPool);

    List<Future<Pair<List<Batch>, List<KvPair>>>> futureList = new ArrayList<>();

    List<Batch> batches =
        getBatches(backOffer, keys, RAW_BATCH_GET_SIZE, MAX_RAW_BATCH_LIMIT, this.clientBuilder);

    Queue<List<Batch>> taskQueue = new LinkedList<>();
    List<KvPair> result = new ArrayList<>();
    taskQueue.offer(batches);

    while (!taskQueue.isEmpty()) {
      List<Batch> task = taskQueue.poll();
      for (Batch batch : task) {
        futureList.add(
            completionService.submit(
                () -> doSendBatchGetInBatchesWithRetry(batch.getBackOffer(), batch)));
      }
      try {
        result.addAll(
            getTasksWithOutput(
                completionService, taskQueue, task, deadline - System.currentTimeMillis()));
      } catch (Exception e) {
        for (Future<Pair<List<Batch>, List<KvPair>>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }

    return result;
  }

  private Pair<List<Batch>, List<KvPair>> doSendBatchGetInBatchesWithRetry(
      BackOffer backOffer, Batch batch) {
    RegionStoreClient client = clientBuilder.build(batch.getRegion(), backOffer);
    try {
      List<KvPair> partialResult = client.rawBatchGet(backOffer, batch.getKeys());
      return Pair.create(new ArrayList<>(), partialResult);
    } catch (final TiKVException e) {
      backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      clientBuilder.getRegionManager().invalidateRegion(batch.getRegion());
      logger.warn("ReSplitting ranges for BatchGetRequest", e);

      // retry
      return Pair.create(doSendBatchGetWithRefetchRegion(backOffer, batch), new ArrayList<>());
    }
  }

  private List<Batch> doSendBatchGetWithRefetchRegion(BackOffer backOffer, Batch batch) {
    return getBatches(
        backOffer, batch.getKeys(), RAW_BATCH_GET_SIZE, MAX_RAW_BATCH_LIMIT, clientBuilder);
  }

  private void doSendBatchDelete(
      BackOffer backOffer, List<ByteString> keys, boolean atomic, long deadline) {
    ExecutorCompletionService<List<Batch>> completionService =
        new ExecutorCompletionService<>(batchDeleteThreadPool);

    List<Future<List<Batch>>> futureList = new ArrayList<>();

    List<Batch> batches =
        getBatches(backOffer, keys, RAW_BATCH_DELETE_SIZE, MAX_RAW_BATCH_LIMIT, this.clientBuilder);

    Queue<List<Batch>> taskQueue = new LinkedList<>();
    taskQueue.offer(batches);

    while (!taskQueue.isEmpty()) {
      List<Batch> task = taskQueue.poll();
      for (Batch batch : task) {
        futureList.add(
            completionService.submit(
                () -> doSendBatchDeleteInBatchesWithRetry(batch.getBackOffer(), batch, atomic)));
      }
      try {
        getTasks(completionService, taskQueue, task, deadline - System.currentTimeMillis());
      } catch (Exception e) {
        for (Future<List<Batch>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }
  }

  private List<Batch> doSendBatchDeleteInBatchesWithRetry(
      BackOffer backOffer, Batch batch, boolean atomic) {
    RegionStoreClient client = clientBuilder.build(batch.getRegion(), backOffer);
    try {
      client.rawBatchDelete(backOffer, batch.getKeys(), atomic);
      return new ArrayList<>();
    } catch (final TiKVException e) {
      backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      clientBuilder.getRegionManager().invalidateRegion(batch.getRegion());
      logger.warn("ReSplitting ranges for BatchGetRequest", e);

      // retry
      return doSendBatchDeleteWithRefetchRegion(backOffer, batch);
    }
  }

  private List<Batch> doSendBatchDeleteWithRefetchRegion(BackOffer backOffer, Batch batch) {
    return getBatches(
        backOffer, batch.getKeys(), RAW_BATCH_DELETE_SIZE, MAX_RAW_BATCH_LIMIT, clientBuilder);
  }

  private ByteString calcKeyByCondition(boolean condition, ByteString key1, ByteString key2) {
    if (condition) {
      return key1;
    }
    return key2;
  }

  private void doSendDeleteRange(
      BackOffer backOffer, ByteString startKey, ByteString endKey, long deadline) {
    ExecutorCompletionService<List<DeleteRange>> completionService =
        new ExecutorCompletionService<>(deleteRangeThreadPool);

    List<Future<List<DeleteRange>>> futureList = new ArrayList<>();

    List<TiRegion> regions = fetchRegionsFromRange(backOffer, startKey, endKey);
    List<DeleteRange> ranges = new ArrayList<>();
    for (int i = 0; i < regions.size(); i++) {
      TiRegion region = regions.get(i);
      ByteString start = calcKeyByCondition(i == 0, startKey, region.getStartKey());
      ByteString end = calcKeyByCondition(i == regions.size() - 1, endKey, region.getEndKey());
      ranges.add(new DeleteRange(backOffer, region, start, end));
    }
    Queue<List<DeleteRange>> taskQueue = new LinkedList<>();
    taskQueue.offer(ranges);
    while (!taskQueue.isEmpty()) {
      List<DeleteRange> task = taskQueue.poll();
      for (DeleteRange range : task) {
        futureList.add(
            completionService.submit(
                () -> doSendDeleteRangeWithRetry(range.getBackOffer(), range)));
      }
      try {
        getTasks(completionService, taskQueue, task, deadline - System.currentTimeMillis());
      } catch (Exception e) {
        for (Future<List<DeleteRange>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }
  }

  private List<DeleteRange> doSendDeleteRangeWithRetry(BackOffer backOffer, DeleteRange range) {
    try (RegionStoreClient client = clientBuilder.build(range.getRegion(), backOffer)) {
      client.setTimeout(conf.getScanTimeout());
      client.rawDeleteRange(backOffer, range.getStartKey(), range.getEndKey());
      return new ArrayList<>();
    } catch (final TiKVException e) {
      backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      clientBuilder.getRegionManager().invalidateRegion(range.getRegion());
      logger.warn("ReSplitting ranges for BatchDeleteRangeRequest", e);

      // retry
      return doSendDeleteRangeWithRefetchRegion(backOffer, range);
    }
  }

  private List<DeleteRange> doSendDeleteRangeWithRefetchRegion(
      BackOffer backOffer, DeleteRange range) {
    List<TiRegion> regions =
        fetchRegionsFromRange(backOffer, range.getStartKey(), range.getEndKey());
    List<DeleteRange> retryRanges = new ArrayList<>();
    for (int i = 0; i < regions.size(); i++) {
      TiRegion region = regions.get(i);
      ByteString start = calcKeyByCondition(i == 0, range.getStartKey(), region.getStartKey());
      ByteString end =
          calcKeyByCondition(i == regions.size() - 1, range.getEndKey(), region.getEndKey());
      retryRanges.add(new DeleteRange(backOffer, region, start, end));
    }
    return retryRanges;
  }

  private static Map<ByteString, ByteString> mapKeysToValues(
      List<ByteString> keys, List<ByteString> values) {
    Map<ByteString, ByteString> map = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      map.put(keys.get(i), values.get(i));
    }
    return map;
  }

  private List<TiRegion> fetchRegionsFromRange(
      BackOffer backOffer, ByteString startKey, ByteString endKey) {
    List<TiRegion> regions = new ArrayList<>();
    while (startKey.isEmpty()
        || endKey.isEmpty()
        || Key.toRawKey(startKey).compareTo(Key.toRawKey(endKey)) < 0) {
      TiRegion currentRegion = clientBuilder.getRegionManager().getRegionByKey(startKey, backOffer);
      regions.add(currentRegion);
      startKey = currentRegion.getEndKey();
      if (currentRegion.getEndKey().isEmpty()) {
        break;
      }
    }
    return regions;
  }

  private Iterator<KvPair> rawScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      int limit,
      boolean keyOnly,
      BackOffer backOffer) {
    if (limit > MAX_RAW_SCAN_LIMIT) {
      throw ERR_MAX_SCAN_LIMIT_EXCEEDED;
    }
    return new RawScanIterator(conf, builder, startKey, endKey, limit, keyOnly, backOffer);
  }
}
