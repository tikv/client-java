/*
 * Copyright 2018 TiKV Project Authors.
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

import static org.tikv.common.util.ClientUtils.appendBatches;
import static org.tikv.common.util.ClientUtils.genUUID;
import static org.tikv.common.util.ClientUtils.getBatches;
import static org.tikv.common.util.ClientUtils.getTasks;
import static org.tikv.common.util.ClientUtils.getTasksWithOutput;
import static org.tikv.common.util.ClientUtils.groupKeysByRegion;

import com.google.protobuf.ByteString;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.RawCASConflictException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.importer.ImporterClient;
import org.tikv.common.importer.SwitchTiKVModeClient;
import org.tikv.common.key.Key;
import org.tikv.common.log.SlowLog;
import org.tikv.common.log.SlowLogEmptyImpl;
import org.tikv.common.log.SlowLogImpl;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.operation.iterator.RawScanIterator;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.Batch;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.DeleteRange;
import org.tikv.common.util.HistogramUtils;
import org.tikv.common.util.Pair;
import org.tikv.common.util.ScanOption;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class RawKVClient implements RawKVClientBase {
  private final TiSession tiSession;
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final boolean atomicForCAS;
  private final ExecutorService batchGetThreadPool;
  private final ExecutorService batchPutThreadPool;
  private final ExecutorService batchDeleteThreadPool;
  private final ExecutorService batchScanThreadPool;
  private final ExecutorService deleteRangeThreadPool;
  private static final Logger logger = LoggerFactory.getLogger(RawKVClient.class);

  public static final Histogram RAW_REQUEST_LATENCY =
      HistogramUtils.buildDuration()
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
    this.tiSession = session;
    this.clientBuilder = clientBuilder;
    this.batchGetThreadPool = session.getThreadPoolForBatchGet();
    this.batchPutThreadPool = session.getThreadPoolForBatchPut();
    this.batchDeleteThreadPool = session.getThreadPoolForBatchDelete();
    this.batchScanThreadPool = session.getThreadPoolForBatchScan();
    this.deleteRangeThreadPool = session.getThreadPoolForDeleteRange();
    this.atomicForCAS = conf.isEnableAtomicForCAS();
  }

  private SlowLog withClusterInfo(SlowLog logger) {
    return logger
        .withField("cluster_id", tiSession.getPDClient().getClusterId())
        .withField("pd_addresses", tiSession.getPDClient().getPdAddrs());
  }

  @Override
  public void close() {}

  @Override
  public void put(ByteString key, ByteString value) {
    put(key, value, 0);
  }

  @Override
  public void put(ByteString key, ByteString value, long ttl) {
    String label = "client_raw_put";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("put");
    span.addProperty("key", KeyUtils.formatBytesUTF8(key));

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog);
    try {
      while (true) {
        try (RegionStoreClient client = clientBuilder.build(key, backOffer)) {
          span.addProperty("region", client.getRegion().toString());
          client.rawPut(backOffer, key, value, ttl, atomicForCAS);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for put error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value) {
    return putIfAbsent(key, value, 0L);
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value, long ttl) {
    try {
      compareAndSet(key, Optional.empty(), value, ttl);
      return Optional.empty();
    } catch (RawCASConflictException e) {
      return e.getPrevValue();
    }
  }

  @Override
  public void compareAndSet(ByteString key, Optional<ByteString> prevValue, ByteString value)
      throws RawCASConflictException {
    compareAndSet(key, prevValue, value, 0L);
  }

  @Override
  public void compareAndSet(
      ByteString key, Optional<ByteString> prevValue, ByteString value, long ttl)
      throws RawCASConflictException {
    if (!atomicForCAS) {
      throw new IllegalArgumentException(
          "To use compareAndSet or putIfAbsent, please enable the config tikv.enable_atomic_for_cas.");
    }

    String label = "client_raw_compare_and_set";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("putIfAbsent");
    span.addProperty("key", KeyUtils.formatBytesUTF8(key));

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog);
    try {
      while (true) {
        try (RegionStoreClient client = clientBuilder.build(key, backOffer)) {
          span.addProperty("region", client.getRegion().toString());
          client.rawCompareAndSet(backOffer, key, prevValue, value, ttl);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for putIfAbsent error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs) {
    batchPut(kvPairs, 0);
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs, long ttl) {
    String label = "client_raw_batch_put";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("batchPut");
    span.addProperty("keySize", String.valueOf(kvPairs.size()));

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVBatchWriteTimeoutInMS(), slowLog);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchWriteTimeoutInMS();
      doSendBatchPut(backOffer, kvPairs, ttl, deadline);
      RAW_REQUEST_SUCCESS.labels(label).inc();
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public Optional<ByteString> get(ByteString key) {
    String label = "client_raw_get";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("get");
    span.addProperty("key", KeyUtils.formatBytesUTF8(key));

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVReadTimeoutInMS(), slowLog);
    try {
      while (true) {
        try (RegionStoreClient client = clientBuilder.build(key, backOffer)) {
          span.addProperty("region", client.getRegion().toString());
          Optional<ByteString> result = client.rawGet(backOffer, key);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return result;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for get error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public List<KvPair> batchGet(List<ByteString> keys) {
    String label = "client_raw_batch_get";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("batchGet");
    span.addProperty("keySize", String.valueOf(keys.size()));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVBatchReadTimeoutInMS(), slowLog);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchReadTimeoutInMS();
      List<KvPair> result = doSendBatchGet(backOffer, keys, deadline);
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public void batchDelete(List<ByteString> keys) {
    String label = "client_raw_batch_delete";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("batchDelete");
    span.addProperty("keySize", String.valueOf(keys.size()));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVBatchWriteTimeoutInMS(), slowLog);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchWriteTimeoutInMS();
      doSendBatchDelete(backOffer, keys, deadline);
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public Optional<Long> getKeyTTL(ByteString key) {
    String label = "client_raw_get_key_ttl";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("getKeyTTL");
    span.addProperty("key", KeyUtils.formatBytesUTF8(key));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVReadTimeoutInMS(), slowLog);
    try {
      while (true) {
        try (RegionStoreClient client = clientBuilder.build(key, backOffer)) {
          span.addProperty("region", client.getRegion().toString());
          Optional<Long> result = client.rawGetKeyTTL(backOffer, key);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return result;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for getKeyTTL error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public List<List<ByteString>> batchScanKeys(
      List<Pair<ByteString, ByteString>> ranges, int eachLimit) {
    return batchScan(
            ranges
                .stream()
                .map(
                    range ->
                        ScanOption.newBuilder()
                            .setStartKey(range.first)
                            .setEndKey(range.second)
                            .setLimit(eachLimit)
                            .setKeyOnly(true)
                            .build())
                .collect(Collectors.toList()))
        .stream()
        .map(kvs -> kvs.stream().map(kv -> kv.getKey()).collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  @Override
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

  @Override
  public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit) {
    return scan(startKey, endKey, limit, false);
  }

  @Override
  public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    String label = "client_raw_scan";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVScanSlowLogInMS()));
    SlowLogSpan span = slowLog.start("scan");
    span.addProperty("startKey", KeyUtils.formatBytesUTF8(startKey));
    span.addProperty("endKey", KeyUtils.formatBytesUTF8(endKey));
    span.addProperty("limit", String.valueOf(limit));
    span.addProperty("keyOnly", String.valueOf(keyOnly));
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
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public List<KvPair> scan(ByteString startKey, int limit) {
    return scan(startKey, limit, false);
  }

  @Override
  public List<KvPair> scan(ByteString startKey, int limit, boolean keyOnly) {
    return scan(startKey, ByteString.EMPTY, limit, keyOnly);
  }

  @Override
  public List<KvPair> scan(ByteString startKey, ByteString endKey) {
    return scan(startKey, endKey, false);
  }

  @Override
  public List<KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly) {
    String label = "client_raw_scan_without_limit";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVScanSlowLogInMS()));
    SlowLogSpan span = slowLog.start("scan");
    span.addProperty("startKey", KeyUtils.formatBytesUTF8(startKey));
    span.addProperty("endKey", KeyUtils.formatBytesUTF8(endKey));
    span.addProperty("keyOnly", String.valueOf(keyOnly));
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
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
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

  @Override
  public List<KvPair> scanPrefix(ByteString prefixKey, int limit, boolean keyOnly) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), limit, keyOnly);
  }

  @Override
  public List<KvPair> scanPrefix(ByteString prefixKey) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString());
  }

  @Override
  public List<KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), keyOnly);
  }

  @Override
  public void delete(ByteString key) {
    String label = "client_raw_delete";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("delete");
    span.addProperty("key", KeyUtils.formatBytesUTF8(key));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog);
    try {
      while (true) {
        try (RegionStoreClient client = clientBuilder.build(key, backOffer)) {
          span.addProperty("region", client.getRegion().toString());
          client.rawDelete(backOffer, key, atomicForCAS);
          RAW_REQUEST_SUCCESS.labels(label).inc();
          return;
        } catch (final TiKVException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
          logger.warn("Retry for delete error", e);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
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

  @Override
  public synchronized void deletePrefix(ByteString key) {
    ByteString endKey = Key.toRawKey(key).nextPrefix().toByteString();
    deleteRange(key, endKey);
  }

  /**
   * Ingest KV pairs to RawKV using StreamKV API.
   *
   * @param list
   */
  public synchronized void ingest(List<Pair<ByteString, ByteString>> list) {
    ingest(list, null);
  }

  /**
   * Ingest KV pairs to RawKV using StreamKV API.
   *
   * @param list
   * @param ttl the ttl of the key (in seconds), 0 means the key will never be outdated
   */
  public synchronized void ingest(List<Pair<ByteString, ByteString>> list, Long ttl)
      throws GrpcException {
    if (list.isEmpty()) {
      return;
    }

    Key min = Key.MAX;
    Key max = Key.MIN;
    Map<ByteString, ByteString> map = new HashMap<>(list.size());

    for (Pair<ByteString, ByteString> pair : list) {
      map.put(pair.first, pair.second);
      Key key = Key.toRawKey(pair.first.toByteArray());
      if (key.compareTo(min) < 0) {
        min = key;
      }
      if (key.compareTo(max) > 0) {
        max = key;
      }
    }

    SwitchTiKVModeClient switchTiKVModeClient = tiSession.getSwitchTiKVModeClient();

    try {
      // switch to normal mode
      switchTiKVModeClient.switchTiKVToNormalMode();

      // region split
      List<byte[]> splitKeys = new ArrayList<>(2);
      splitKeys.add(min.getBytes());
      splitKeys.add(max.next().getBytes());
      tiSession.splitRegionAndScatter(splitKeys);
      tiSession.getRegionManager().invalidateAll();

      // switch to import mode
      switchTiKVModeClient.keepTiKVToImportMode();

      // group keys by region
      List<ByteString> keyList = list.stream().map(pair -> pair.first).collect(Collectors.toList());
      Map<TiRegion, List<ByteString>> groupKeys =
          groupKeysByRegion(clientBuilder.getRegionManager(), keyList, defaultBackOff());

      // ingest for each region
      for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
        TiRegion region = entry.getKey();
        List<ByteString> keys = entry.getValue();
        List<Pair<ByteString, ByteString>> kvs =
            keys.stream().map(k -> Pair.create(k, map.get(k))).collect(Collectors.toList());
        doIngest(region, kvs, ttl);
      }
    } finally {
      // swith tikv to normal mode
      switchTiKVModeClient.stopKeepTiKVToImportMode();
      switchTiKVModeClient.switchTiKVToNormalMode();
    }
  }

  private void doIngest(TiRegion region, List<Pair<ByteString, ByteString>> sortedList, Long ttl)
      throws GrpcException {
    if (sortedList.isEmpty()) {
      return;
    }

    ByteString uuid = ByteString.copyFrom(genUUID());
    Key minKey = Key.toRawKey(sortedList.get(0).first);
    Key maxKey = Key.toRawKey(sortedList.get(sortedList.size() - 1).first);
    ImporterClient importerClient =
        new ImporterClient(tiSession, uuid, minKey, maxKey, region, ttl);
    importerClient.write(sortedList.iterator());
  }

  private void doSendBatchPut(
      BackOffer backOffer, Map<ByteString, ByteString> kvPairs, long ttl, long deadline) {
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
        completionService.submit(
            () -> doSendBatchPutInBatchesWithRetry(batch.getBackOffer(), batch, ttl));

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
  }

  private List<Batch> doSendBatchPutInBatchesWithRetry(BackOffer backOffer, Batch batch, long ttl) {
    try (RegionStoreClient client = clientBuilder.build(batch.getRegion(), backOffer)) {
      client.setTimeout(conf.getRawKVBatchWriteTimeoutInMS());
      client.rawBatchPut(backOffer, batch, ttl, atomicForCAS);
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

    try (RegionStoreClient client = clientBuilder.build(batch.getRegion(), backOffer)) {
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

  private void doSendBatchDelete(BackOffer backOffer, List<ByteString> keys, long deadline) {
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
                () -> doSendBatchDeleteInBatchesWithRetry(batch.getBackOffer(), batch)));
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

  private List<Batch> doSendBatchDeleteInBatchesWithRetry(BackOffer backOffer, Batch batch) {
    try (RegionStoreClient client = clientBuilder.build(batch.getRegion(), backOffer)) {
      client.rawBatchDelete(backOffer, batch.getKeys(), atomicForCAS);
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

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return iterator of key-value pairs in range
   */
  public Iterator<KvPair> scan0(ByteString startKey, ByteString endKey, int limit) {
    return scan0(startKey, endKey, limit, false);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return iterator of key-value pairs in range
   */
  public Iterator<KvPair> scan0(ByteString startKey, int limit) {
    return scan0(startKey, limit, false);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return iterator of key-value pairs in range
   */
  public Iterator<KvPair> scan0(ByteString startKey, int limit, boolean keyOnly) {
    return scan0(startKey, ByteString.EMPTY, limit, keyOnly);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return iterator of key-value pairs in range
   */
  public Iterator<KvPair> scan0(
      ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    String label = "client_raw_scan";
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(label).startTimer();
    try {
      Iterator<KvPair> iterator =
          rawScanIterator(conf, clientBuilder, startKey, endKey, limit, keyOnly, defaultBackOff());
      RAW_REQUEST_SUCCESS.labels(label).inc();
      return iterator;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(label).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return iterator of key-value pairs in range
   */
  public Iterator<KvPair> scan0(ByteString startKey, ByteString endKey) {
    return scan0(startKey, endKey, false);
  }

  private Iterator<KvPair> scan0(ScanOption scanOption) {
    ByteString startKey = scanOption.getStartKey();
    ByteString endKey = scanOption.getEndKey();
    int limit = scanOption.getLimit();
    boolean keyOnly = scanOption.isKeyOnly();
    return scan0(startKey, endKey, limit, keyOnly);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param limit limit of keys retrieved
   * @param keyOnly whether to scan in keyOnly mode
   * @return kvPairs iterator with the specified prefix
   */
  public Iterator<KvPair> scanPrefix0(ByteString prefixKey, int limit, boolean keyOnly) {
    return scan0(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), limit, keyOnly);
  }

  public Iterator<KvPair> scanPrefix0(ByteString prefixKey) {
    return scan0(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString());
  }

  public Iterator<KvPair> scanPrefix0(ByteString prefixKey, boolean keyOnly) {
    return scan0(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), keyOnly);
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param keyOnly whether to scan in key-only mode
   * @return iterator of key-value pairs in range
   */
  public Iterator<KvPair> scan0(ByteString startKey, ByteString endKey, boolean keyOnly) {
    return new TikvIterator(startKey, endKey, keyOnly);
  }

  public class TikvIterator implements Iterator<KvPair> {

    private Iterator<KvPair> iterator;

    private final ByteString startKey;
    private final ByteString endKey;
    private final boolean keyOnly;

    private KvPair last;

    public TikvIterator(ByteString startKey, ByteString endKey, boolean keyOnly) {
      this.startKey = startKey;
      this.endKey = endKey;
      this.keyOnly = keyOnly;

      this.iterator =
          rawScanIterator(
              conf,
              clientBuilder,
              this.startKey,
              this.endKey,
              conf.getScanBatchSize(),
              keyOnly,
              defaultBackOff());
    }

    @Override
    public boolean hasNext() {
      if (this.iterator.hasNext()) {
        return true;
      }
      if (this.last == null) {
        return false;
      }
      ByteString startKey = Key.toRawKey(this.last.getKey()).next().toByteString();
      this.iterator =
          rawScanIterator(
              conf,
              clientBuilder,
              startKey,
              endKey,
              conf.getScanBatchSize(),
              keyOnly,
              defaultBackOff());
      this.last = null;
      return this.iterator.hasNext();
    }

    @Override
    public KvPair next() {
      KvPair next = this.iterator.next();
      this.last = next;
      return next;
    }
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(conf.getRawKVDefaultBackoffInMS());
  }
}
