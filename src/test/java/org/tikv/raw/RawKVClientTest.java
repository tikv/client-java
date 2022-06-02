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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.BaseRawKVTest;
import org.tikv.common.StoreConfig;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.log.SlowLogEmptyImpl;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.common.util.Pair;
import org.tikv.common.util.ScanOption;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class RawKVClientTest extends BaseRawKVTest {
  private static final String RAW_PREFIX = "raw_\u0001_";
  private static final int KEY_POOL_SIZE = 1000000;
  private static final int TEST_CASES = 10000;
  private static final int WORKER_CNT = 100;
  private static final ByteString RAW_START_KEY = ByteString.copyFromUtf8(RAW_PREFIX);
  private static final ByteString RAW_END_KEY =
      Key.toRawKey(RAW_START_KEY).nextPrefix().toByteString();
  private RawKVClient client;
  private static final List<ByteString> orderedKeys;
  private static final List<ByteString> randomKeys;
  private static final List<ByteString> values;
  private static final int limit;
  private TreeMap<ByteString, ByteString> data;
  private boolean initialized;
  private final Random r = new Random(1234);
  private static final ByteStringComparator bsc = new ByteStringComparator();
  private static final ExecutorService executors = Executors.newFixedThreadPool(WORKER_CNT);
  private final ExecutorCompletionService<Object> completionService =
      new ExecutorCompletionService<>(executors);
  private static final Logger logger = LoggerFactory.getLogger(RawKVClientTest.class);
  private TiSession session;

  static {
    orderedKeys = new ArrayList<>();
    randomKeys = new ArrayList<>();
    values = new ArrayList<>();
    limit = 10000;
    for (int i = 0; i < KEY_POOL_SIZE; i++) {
      orderedKeys.add(rawKey(String.valueOf(i)));
      randomKeys.add(getRandomRawKey());
      values.add(getRandomValue());
    }
  }

  private static String getRandomString() {
    return RandomStringUtils.randomAlphanumeric(7, 18);
  }

  private static ByteString getRandomRawKey() {
    return rawKey(getRandomString());
  }

  private static ByteString getRandomValue() {
    return ByteString.copyFrom(getRandomString().getBytes());
  }

  @Before
  public void setup() {
    try {
      TiConfiguration conf = createTiConfiguration();
      session = TiSession.create(conf);
      initialized = false;
      if (client == null) {
        client = session.createRawClient();
      }
      data = new TreeMap<>(bsc);
      initialized = true;
    } catch (Exception e) {
      logger.warn(
          "Cannot initialize raw client, please check whether TiKV is running. Test skipped.", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void getKeyTTLTest() {
    long ttl = 10;
    ByteString key = ByteString.copyFromUtf8("key_ttl");
    ByteString value = ByteString.copyFromUtf8("value");
    client.put(key, value, ttl);
    for (int i = 0; i < 9; i++) {
      Optional<Long> t = client.getKeyTTL(key);
      logger.info("current ttl of key is " + t.orElse(null));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Optional<Long> t = client.getKeyTTL(key);
    if (t.isPresent()) {
      logger.info("key not outdated: " + t.get());
    } else {
      logger.info("key outdated.");
    }
  }

  private ByteString generateBatchPutKey(String envId, String type, String id) {
    return ByteString.copyFromUtf8(
        String.format("indexInfo_:_{%s}_:_{%s}_:_{%s}", envId, type, id));
  }

  private ByteString generateBatchPutValue() {
    return ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(290));
  }

  private String generateEnvId() {
    return String.format(
        "%s%02d", RandomStringUtils.randomAlphabetic(2).toLowerCase(Locale.ROOT), r.nextInt(100));
  }

  private String generateType() {
    return String.format(
        "%s%02d", RandomStringUtils.randomAlphabetic(3).toUpperCase(Locale.ROOT), r.nextInt(10000));
  }

  @Test
  public void testCustomBackOff() {
    int timeout = 2000;
    int sleep = 150;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(timeout);
    long s = System.currentTimeMillis();
    try {
      while (true) {
        Thread.sleep(sleep);
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, new Exception("t"));
      }
    } catch (Exception ignored) {
    } finally {
      long e = System.currentTimeMillis();
      long duration = e - s;
      assertTrue(duration >= 2900);
    }
  }

  @Test
  public void testDeadlineBackOff() {
    int timeout = 2000;
    int sleep = 150;
    BackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(timeout, SlowLogEmptyImpl.INSTANCE, 0);
    long s = System.currentTimeMillis();
    try {
      while (true) {
        Thread.sleep(sleep);
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, new Exception("t"));
      }
    } catch (Exception ignored) {
    } finally {
      long e = System.currentTimeMillis();
      long duration = e - s;
      assertTrue(duration <= timeout + sleep);
    }
  }

  @Test
  public void testBackoffTimeout() {
    int timeout = 500;
    int sleep = 150;
    BackOffer backOffer = ConcreteBackOffer.newDeadlineBackOff(timeout, SlowLogEmptyImpl.INSTANCE);
    long s = System.currentTimeMillis();
    try {
      while (true) {
        Thread.sleep(sleep);
        backOffer.checkTimeout();
      }
    } catch (Exception ignored) {
    } finally {
      long e = System.currentTimeMillis();
      long duration = e - s;
      assertTrue(duration <= timeout + sleep);
    }
  }

  @Test
  public void batchPutTest() {
    ExecutorService executors = Executors.newFixedThreadPool(200);
    ExecutorCompletionService<Object> completionService =
        new ExecutorCompletionService<>(executors);
    long dataCnt = Long.parseLong(System.getProperty("tikv.test.batch_put_cnt", "1000"));
    long keysPerBatch = 1000;

    long workerCnt = dataCnt / keysPerBatch;

    List<String> envIdPool = new ArrayList<>();
    int envIdPoolSize = 10000;
    for (int i = 0; i < envIdPoolSize; i++) {
      envIdPool.add(generateEnvId());
    }

    List<String> typePool = new ArrayList<>();
    int typePoolSize = 10000;
    for (int i = 0; i < typePoolSize; i++) {
      typePool.add(generateType());
    }

    List<ByteString> valuePool = new ArrayList<>();
    int valuePoolSize = 10000;
    for (int i = 0; i < valuePoolSize; i++) {
      valuePool.add(generateBatchPutValue());
    }

    boolean randomBatchPutTest =
        System.getProperty("tikv.test.batch_put_strategy", "random").equalsIgnoreCase("random");

    if (randomBatchPutTest) {
      for (long i = 0; i < workerCnt; i++) {
        completionService.submit(
            () -> {
              String envId = envIdPool.get(r.nextInt(envIdPoolSize));
              String type = typePool.get(r.nextInt(typePoolSize));
              String prefix =
                  String.format(
                      "%d%09d%09d", r.nextInt(10), r.nextInt(1000000000), r.nextInt(1000000000));
              Map<ByteString, ByteString> map = new HashMap<>();
              RawKVClient rawKVClient = session.createRawClient();
              for (int j = 0; j < keysPerBatch; j++) {
                String id = String.format("%s%04d", prefix, j);
                map.put(
                    generateBatchPutKey(envId, type, id), valuePool.get(r.nextInt(valuePoolSize)));
              }
              rawKVClient.batchPut(map);
              return null;
            });
      }
    } else {
      for (long i = 0; i < workerCnt; i++) {
        final long idx = i;
        completionService.submit(
            () -> {
              String envId = envIdPool.get(0);
              String type = typePool.get(0);
              String prefix = String.format("%d%09d%06d", 0, 0, idx / keysPerBatch);
              Map<ByteString, ByteString> map = new HashMap<>();
              RawKVClient rawKVClient = session.createRawClient();
              long offset = idx % keysPerBatch;
              for (int j = 0; j < keysPerBatch; j++) {
                String id = String.format("%s%03d%04d", prefix, offset, j);
                map.put(
                    generateBatchPutKey(envId, type, id), valuePool.get(r.nextInt(valuePoolSize)));
              }
              rawKVClient.batchPut(map);
              return null;
            });
      }
    }
    logger.info("start");
    try {
      for (int i = 0; i < workerCnt; i++) {
        completionService.take().get(1, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("Current thread interrupted. Test fail.");
    } catch (TimeoutException e) {
      logger.info("TimeOut Exceeded for current test. " + 1 + "s");
    } catch (ExecutionException e) {
      logger.info("Execution exception met. Test fail.");
    }
    logger.info("done");
  }

  @Test
  public void deleteRangeTest() {
    checkDeleteRange(ByteString.EMPTY, ByteString.EMPTY);
  }

  @Test
  public void batchDeleteTest() {
    int cnt = 8;
    List<ByteString> keys = new ArrayList<>();
    for (int i = 0; i < cnt; i++) {
      ByteString key = getRandomRawKey().concat(ByteString.copyFromUtf8("batch_delete_test"));
      client.put(key, key);
      keys.add(key);
    }

    client.batchDelete(keys);

    for (int i = 0; i < cnt; i++) {
      checkNotExist(keys.get(i));
    }
  }

  @Test
  public void scan0test() {
    int cnt = 8;
    ByteString prefix = ByteString.copyFromUtf8("scan0_test");
    client.deletePrefix(prefix);
    List<ByteString> keys = new ArrayList<>();
    for (int i = 0; i < cnt; i++) {
      ByteString key = prefix.concat(getRandomRawKey());
      client.put(key, key);
      keys.add(key);
    }

    int i = 0;
    Iterator<KvPair> iter = client.scan0(prefix, ByteString.EMPTY, cnt);
    while (iter.hasNext()) {
      i++;
      KvPair pair = iter.next();
      assertEquals(pair.getKey(), pair.getValue());
    }
    assertEquals(cnt, i);

    i = 0;
    iter = client.scan0(prefix, ByteString.EMPTY, true);
    while (iter.hasNext()) {
      i++;
      KvPair pair = iter.next();
      assertEquals(pair.getValue(), ByteString.EMPTY);
    }
    assertEquals(cnt, i);
  }

  @Test
  public void ingestTest() {
    Assume.assumeTrue(tikvVersionNewerThan("5.2.0"));
    int cnt = 8;
    ByteString prefix = ByteString.copyFromUtf8("ingest_test");
    client.deletePrefix(prefix);
    List<Pair<ByteString, ByteString>> kvs = new ArrayList<>();
    for (int i = 0; i < cnt; i++) {
      ByteString key = prefix.concat(getRandomRawKey());
      kvs.add(Pair.create(key, key));
    }
    kvs.sort(
        (o1, o2) -> {
          Key k1 = Key.toRawKey(o1.first.toByteArray());
          Key k2 = Key.toRawKey(o2.first.toByteArray());
          return k1.compareTo(k2);
        });
    client.ingest(kvs);

    assertEquals(client.scan(prefix, ByteString.EMPTY).size(), cnt);
  }

  @Test
  public void simpleTest() {
    ByteString key = rawKey("key");
    ByteString key0 = rawKey("key0");
    ByteString key1 = rawKey("key1");
    ByteString key2 = rawKey("key2");
    ByteString key3 = rawKey("key3");
    ByteString value = rawValue("value");
    ByteString value1 = rawValue("value1");
    ByteString value2 = rawValue("value2");
    ByteString value3 = rawValue("value3");
    Kvrpcpb.KvPair kv = Kvrpcpb.KvPair.newBuilder().setKey(key).setValue(value).build();
    Kvrpcpb.KvPair kv1 = Kvrpcpb.KvPair.newBuilder().setKey(key1).setValue(value1).build();
    Kvrpcpb.KvPair kv2 = Kvrpcpb.KvPair.newBuilder().setKey(key2).setValue(value2).build();
    Kvrpcpb.KvPair kv3 = Kvrpcpb.KvPair.newBuilder().setKey(key3).setValue(value3).build();

    try {
      checkDeleteRange(ByteString.EMPTY, ByteString.EMPTY);
      checkNotExist(key);
      checkNotExist(key1);
      checkNotExist(key2);
      checkNotExist(key3);
      checkPut(kv);
      checkPut(kv1);
      checkPut(kv2);
      checkPut(kv3);
      // <key, value>, <key1,value1>, <key2,value2>, <key3,value3>
      // (-∞, +∞)
      checkScan(ByteString.EMPTY, ByteString.EMPTY, Arrays.asList(kv, kv1, kv2, kv3), limit);
      // (-∞, key3)
      checkScan(ByteString.EMPTY, key3, Arrays.asList(kv, kv1, kv2), limit);
      // [key1, +∞)
      checkScan(key1, ByteString.EMPTY, Arrays.asList(kv1, kv2, kv3), limit);
      // [key, key3)
      checkScan(key, key3, Arrays.asList(kv, kv1, kv2), limit);
      // [key1, key3)
      checkScan(key1, key3, Arrays.asList(kv1, kv2), limit);
      // [key0, key1)
      checkScan(key0, key1, new ArrayList<>(), limit);
      // [key, key2)
      checkScan(key, key2, Arrays.asList(kv, kv1), limit);
      checkDelete(key1);
      checkDelete(key2);
      checkDeleteRange(ByteString.EMPTY, ByteString.EMPTY);
    } catch (final TiKVException e) {
      logger.warn("Test fails with Exception: " + e);
    }
  }

  private List<Kvrpcpb.KvPair> rawKeys() {
    return client.scan(RAW_START_KEY, RAW_END_KEY);
  }

  @Test
  public void scanTestForIssue540() {
    ByteString splitKeyA = ByteString.copyFromUtf8("splitKeyA");
    ByteString splitKeyB = ByteString.copyFromUtf8("splitKeyB");
    session.splitRegionAndScatter(
        ImmutableList.of(splitKeyA.toByteArray(), splitKeyB.toByteArray()));
    client.deleteRange(ByteString.EMPTY, ByteString.EMPTY);

    client.put(ByteString.EMPTY, ByteString.EMPTY);
    client.put(splitKeyA, ByteString.EMPTY);
    Assert.assertEquals(0, client.scan(ByteString.EMPTY, 0).size());
    Assert.assertEquals(1, client.scan(ByteString.EMPTY, 1).size());
    Assert.assertEquals(2, client.scan(ByteString.EMPTY, 2).size());
    Assert.assertEquals(2, client.scan(ByteString.EMPTY, 3).size());

    client.deleteRange(ByteString.EMPTY, ByteString.EMPTY);

    client.put(ByteString.EMPTY, ByteString.EMPTY);
    client.put(splitKeyA, ByteString.EMPTY);
    client.put(splitKeyA.concat(ByteString.copyFromUtf8("1")), ByteString.EMPTY);
    client.put(splitKeyA.concat(ByteString.copyFromUtf8("2")), ByteString.EMPTY);
    client.put(splitKeyA.concat(ByteString.copyFromUtf8("3")), ByteString.EMPTY);
    client.put(splitKeyB.concat(ByteString.copyFromUtf8("1")), ByteString.EMPTY);
    Assert.assertEquals(6, client.scan(ByteString.EMPTY, 7).size());
    Assert.assertEquals(0, client.scan(ByteString.EMPTY, -1).size());
    client.deleteRange(ByteString.EMPTY, ByteString.EMPTY);
  }

  @Test
  public void validate() {
    baseTest(100, 100, 100, 100, false, false, false, false, false);
    baseTest(100, 100, 100, 100, false, true, true, true, true);
  }

  /** Example of benchmarking base test */
  public void benchmark() {
    baseTest(TEST_CASES, TEST_CASES, 200, 5000, true, false, false, false, false);
    baseTest(TEST_CASES, TEST_CASES, 200, 5000, true, true, true, true, true);
  }

  private void baseTest(
      int putCases,
      int getCases,
      int scanCases,
      int deleteCases,
      boolean benchmark,
      boolean batchPut,
      boolean batchGet,
      boolean batchScan,
      boolean deleteRange) {
    if (putCases > KEY_POOL_SIZE) {
      logger.info("Number of distinct orderedKeys required exceeded pool size " + KEY_POOL_SIZE);
      return;
    }
    if (deleteCases > putCases) {
      logger.info("Number of orderedKeys to delete is more than total number of orderedKeys");
      return;
    }

    try {
      prepare();

      if (batchPut) {
        rawBatchPutTest(putCases, benchmark);
      } else {
        rawPutTest(putCases, benchmark);
      }
      if (batchGet) {
        rawBatchGetTest(getCases, benchmark);
      } else {
        rawGetTest(getCases, benchmark);
      }
      if (batchScan) {
        rawBatchScanTest(scanCases, benchmark);
      } else {
        rawScanTest(scanCases, benchmark);
      }
      if (deleteRange) {
        rawDeleteRangeTest(benchmark);
      } else {
        rawDeleteTest(deleteCases, benchmark);
      }

      long ttl = 10;
      rawTTLTest(10, ttl, benchmark);

      prepare();
    } catch (final TiKVException e) {
      logger.warn("Test fails with Exception " + e);
    }
    logger.info("ok, test done");
  }

  private void prepare() {
    logger.info("Initializing test");
    data.clear();
    List<Kvrpcpb.KvPair> remainingKeys = rawKeys();
    int sz = remainingKeys.size();
    logger.info("deleting " + sz);
    int base = sz / WORKER_CNT;
    remainingKeys.forEach(
        kvPair -> {
          try {
            checkDelete(kvPair.getKey());
          } catch (final TiKVException e) {
            logger.warn(
                "kvPair check delete fails: "
                    + KeyUtils.formatBytes(kvPair.getKey())
                    + ", "
                    + KeyUtils.formatBytes(kvPair.getValue()));
          }
        });
    for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
      int i = cnt;
      completionService.submit(
          () -> {
            for (int j = 0; j < base; j++) {
              checkDelete(remainingKeys.get(i * base + j).getKey());
            }
            return null;
          });
    }
    awaitTimeOut(base / 100);
  }

  private void awaitTimeOut(int timeOutLimit) {
    try {
      for (int i = 0; i < WORKER_CNT; i++) {
        completionService.take().get(timeOutLimit, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("Current thread interrupted. Test fail.");
    } catch (TimeoutException e) {
      logger.info("TimeOut Exceeded for current test. " + timeOutLimit + "s");
    } catch (ExecutionException e) {
      logger.info("Execution exception met. Test fail.");
    }
  }

  private void rawPutTest(int putCases, boolean benchmark) {
    logger.info("put testing");
    if (benchmark) {
      for (int i = 0; i < putCases; i++) {
        ByteString key = orderedKeys.get(i), value = values.get(i);
        data.put(key, value);
      }

      long start = System.currentTimeMillis();
      int base = putCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString key = orderedKeys.get(num), value = values.get(num);
                client.put(key, value);
              }
              return null;
            });
      }
      awaitTimeOut(100);
      long end = System.currentTimeMillis();
      logger.info(
          putCases
              + " put: "
              + (end - start) / 1000.0
              + "s workers="
              + WORKER_CNT
              + " put="
              + rawKeys().size());
    } else {
      for (int i = 0; i < putCases; i++) {
        ByteString key = randomKeys.get(i), value = values.get(r.nextInt(KEY_POOL_SIZE));
        data.put(key, value);
        checkPut(key, value);
      }
    }
  }

  private void rawBatchPutTest(int putCases, boolean benchmark) {
    logger.info("batchPut testing");
    if (benchmark) {
      for (int i = 0; i < putCases; i++) {
        ByteString key = orderedKeys.get(i), value = values.get(i);
        data.put(key, value);
      }

      long start = System.currentTimeMillis();
      int base = putCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              Map<ByteString, ByteString> kvPairs = new HashMap<>();
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString key = orderedKeys.get(num), value = values.get(num);
                kvPairs.put(key, value);
              }
              client.batchPut(kvPairs);
              return null;
            });
      }
      awaitTimeOut(100);
      long end = System.currentTimeMillis();
      logger.info(
          putCases
              + " batchPut: "
              + (end - start) / 1000.0
              + "s workers="
              + WORKER_CNT
              + " put="
              + rawKeys().size());
    } else {
      Map<ByteString, ByteString> kvPairs = new HashMap<>();
      for (int i = 0; i < putCases; i++) {
        ByteString key = randomKeys.get(i), value = values.get(r.nextInt(KEY_POOL_SIZE));
        data.put(key, value);
        kvPairs.put(key, value);
      }
      checkBatchPut(kvPairs);
    }
  }

  private void rawGetTest(int getCases, boolean benchmark) {
    logger.info("get testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = getCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString key = orderedKeys.get(num);
                client.get(key);
              }
              return null;
            });
      }
      awaitTimeOut(200);
      long end = System.currentTimeMillis();
      logger.info(getCases + " get: " + (end - start) / 1000.0 + "s");
    } else {
      int i = 0;
      for (Map.Entry<ByteString, ByteString> pair : data.entrySet()) {
        assertEquals(client.get(pair.getKey()), Optional.of(pair.getValue()));
        i++;
        if (i >= getCases) {
          break;
        }
      }
    }
  }

  private void rawBatchGetTest(int getCases, boolean benchmark) {
    logger.info("batchGet testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = getCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              List<ByteString> keys = new ArrayList<>();
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString key = orderedKeys.get(num);
                keys.add(key);
              }
              client.batchGet(keys);
              return null;
            });
      }
      awaitTimeOut(200);
      long end = System.currentTimeMillis();
      logger.info(getCases + " batchGet: " + (end - start) / 1000.0 + "s");
    } else {
      int i = 0;
      List<ByteString> keys = new ArrayList<>();
      for (Map.Entry<ByteString, ByteString> pair : data.entrySet()) {
        keys.add(pair.getKey());
        i++;
        if (i >= getCases) {
          break;
        }
      }
      checkBatchGet(keys);
    }
  }

  private void rawScanTest(int scanCases, boolean benchmark) {
    logger.info("scan testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = scanCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString startKey = randomKeys.get(num), endKey = randomKeys.get(num + 1);
                if (bsc.compare(startKey, endKey) > 0) {
                  ByteString tmp = startKey;
                  startKey = endKey;
                  endKey = tmp;
                }
                client.scan(startKey, endKey, limit);
              }
              return null;
            });
      }
      awaitTimeOut(200);
      long end = System.currentTimeMillis();
      logger.info(scanCases + " scan: " + (end - start) / 1000.0 + "s");
    } else {
      for (int i = 0; i < scanCases; i++) {
        ByteString startKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE)),
            endKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE));
        if (bsc.compare(startKey, endKey) > 0) {
          ByteString tmp = startKey;
          startKey = endKey;
          endKey = tmp;
        }
        checkScan(startKey, endKey, data, limit);
      }
    }
  }

  private void rawBatchScanTest(int scanCases, boolean benchmark) {
    logger.info("batchScan testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = scanCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              List<ScanOption> scanOptions = new ArrayList<>();
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString startKey = randomKeys.get(num), endKey = randomKeys.get(num + 1);
                if (bsc.compare(startKey, endKey) > 0) {
                  ByteString tmp = startKey;
                  startKey = endKey;
                  endKey = tmp;
                }
                ScanOption scanOption =
                    ScanOption.newBuilder()
                        .setStartKey(startKey)
                        .setEndKey(endKey)
                        .setLimit(limit)
                        .build();
                scanOptions.add(scanOption);
              }
              client.batchScan(scanOptions);
              return null;
            });
      }
      awaitTimeOut(200);
      long end = System.currentTimeMillis();
      logger.info(scanCases + " batchScan: " + (end - start) / 1000.0 + "s");
    } else {
      List<ScanOption> scanOptions = new ArrayList<>();
      for (int i = 0; i < scanCases; i++) {
        ByteString startKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE)),
            endKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE));
        if (bsc.compare(startKey, endKey) > 0) {
          ByteString tmp = startKey;
          startKey = endKey;
          endKey = tmp;
        }
        ScanOption scanOption =
            ScanOption.newBuilder().setStartKey(startKey).setEndKey(endKey).setLimit(limit).build();
        scanOptions.add(scanOption);
      }
      checkBatchScan(scanOptions);
      checkBatchScanKeys(
          scanOptions
              .stream()
              .map(scanOption -> Pair.create(scanOption.getStartKey(), scanOption.getEndKey()))
              .collect(Collectors.toList()));
    }
  }

  private void rawDeleteTest(int deleteCases, boolean benchmark) {
    logger.info("delete testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      int base = deleteCases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString key = orderedKeys.get(num);
                client.delete(key);
              }
              return null;
            });
      }
      awaitTimeOut(100);
      long end = System.currentTimeMillis();
      logger.info(deleteCases + " delete: " + (end - start) / 1000.0 + "s");
    } else {
      int i = 0;
      for (ByteString key : data.keySet()) {
        checkDelete(key);
        i++;
        if (i >= deleteCases) {
          break;
        }
      }
    }
  }

  private void rawDeleteRangeTest(boolean benchmark) {
    logger.info("deleteRange testing");
    if (benchmark) {
      long start = System.currentTimeMillis();
      ByteString startKey = randomKeys.get(0), endKey = randomKeys.get(1);
      if (bsc.compare(startKey, endKey) > 0) {
        ByteString tmp = startKey;
        startKey = endKey;
        endKey = tmp;
      }
      client.deleteRange(startKey, endKey);
      long end = System.currentTimeMillis();
      logger.info("deleteRange: " + (end - start) / 1000.0 + "s");
    } else {
      ByteString startKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE)),
          endKey = randomKeys.get(r.nextInt(KEY_POOL_SIZE));
      if (bsc.compare(startKey, endKey) > 0) {
        ByteString tmp = startKey;
        startKey = endKey;
        endKey = tmp;
      }
      checkDeleteRange(startKey, endKey);
    }
  }

  public void rawTTLTest(int cases, long ttl, boolean benchmark) {
    Assume.assumeTrue(StoreConfig.ifTllEnable(session.getPDClient()));
    logger.info("ttl testing");
    if (benchmark) {
      for (int i = 0; i < cases; i++) {
        ByteString key = orderedKeys.get(i), value = values.get(i);
        data.put(key, value);
      }

      long start = System.currentTimeMillis();
      int base = cases / WORKER_CNT;
      for (int cnt = 0; cnt < WORKER_CNT; cnt++) {
        int i = cnt;
        completionService.submit(
            () -> {
              for (int j = 0; j < base; j++) {
                int num = i * base + j;
                ByteString key = orderedKeys.get(num), value = values.get(num);
                client.put(key, value, ttl);
              }
              return null;
            });
      }
      awaitTimeOut(100);
      long end = System.currentTimeMillis();
      logger.info(
          cases
              + " ttl put: "
              + (end - start) / 1000.0
              + "s workers="
              + WORKER_CNT
              + " put="
              + rawKeys().size());
    } else {
      for (int i = 0; i < cases; i++) {
        ByteString key = randomKeys.get(i), value = values.get(r.nextInt(KEY_POOL_SIZE));
        data.put(key, value);
        checkPutTTL(key, value, ttl);
        checkGetKeyTTL(key, ttl);
      }
      try {
        Thread.sleep(ttl * 1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      for (int i = 0; i < cases; i++) {
        ByteString key = randomKeys.get(i);
        checkGetTTLTimeOut(key);
        checkGetKeyTTLTimeOut(key);
      }
    }
  }

  private void checkBatchGet(List<ByteString> keys) {
    List<Kvrpcpb.KvPair> result = client.batchGet(keys);
    for (Kvrpcpb.KvPair kvPair : result) {
      assertTrue(data.containsKey(kvPair.getKey()));
      assertEquals(data.get(kvPair.getKey()), kvPair.getValue());
    }
  }

  private void checkPut(Kvrpcpb.KvPair kv) {
    checkPut(kv.getKey(), kv.getValue());
  }

  private void checkPut(ByteString key, ByteString value) {
    client.put(key, value);
    assertEquals(client.get(key).orElse(null), value);
  }

  private void checkBatchPut(Map<ByteString, ByteString> kvPairs) {
    client.batchPut(kvPairs);
    for (Map.Entry<ByteString, ByteString> kvPair : kvPairs.entrySet()) {
      assertEquals(client.get(kvPair.getKey()).orElse(null), kvPair.getValue());
    }
  }

  private void checkScan(
      ByteString startKey, ByteString endKey, List<Kvrpcpb.KvPair> expected, int limit) {
    List<Kvrpcpb.KvPair> result = client.scan(startKey, endKey, limit);
    assertEquals(expected, result);
  }

  private void checkScan(
      ByteString startKey, ByteString endKey, TreeMap<ByteString, ByteString> data, int limit) {
    checkScan(
        startKey,
        endKey,
        data.subMap(startKey, endKey)
            .entrySet()
            .stream()
            .map(
                kvPair ->
                    Kvrpcpb.KvPair.newBuilder()
                        .setKey(kvPair.getKey())
                        .setValue(kvPair.getValue())
                        .build())
            .collect(Collectors.toList()),
        limit);
  }

  private void checkBatchScan(List<ScanOption> scanOptions) {
    logger.info("checking batch scan");
    List<List<Kvrpcpb.KvPair>> result = client.batchScan(scanOptions);
    int i = 0;
    for (ScanOption scanOption : scanOptions) {
      List<Kvrpcpb.KvPair> partialResult =
          data.subMap(scanOption.getStartKey(), scanOption.getEndKey())
              .entrySet()
              .stream()
              .map(
                  kvPair ->
                      Kvrpcpb.KvPair.newBuilder()
                          .setKey(kvPair.getKey())
                          .setValue(kvPair.getValue())
                          .build())
              .collect(Collectors.toList());
      assertEquals(partialResult, result.get(i));
      i++;
    }
  }

  private void checkBatchScanKeys(List<Pair<ByteString, ByteString>> ranges) {
    logger.info("checking batch scan keys");
    List<List<ByteString>> result = client.batchScanKeys(ranges, limit);
    for (int i = 0; i < ranges.size(); i++) {
      Pair<ByteString, ByteString> range = ranges.get(i);
      List<ByteString> partialResult =
          new ArrayList<>(data.subMap(range.first, range.second).keySet());
      assertEquals(partialResult, result.get(i));
    }
  }

  private void checkDelete(ByteString key) {
    client.delete(key);
    checkNotExist(key);
  }

  private void checkDeleteRange(ByteString startKey, ByteString endKey) {
    client.deleteRange(startKey, endKey);
    logger.info("delete range complete");
    List<Kvrpcpb.KvPair> result = client.scan(startKey, endKey);
    logger.info("checking scan complete. number of remaining keys in range: " + result.size());
    assertTrue(result.isEmpty());
  }

  private void checkPutTTL(ByteString key, ByteString value, long ttl) {
    client.put(key, value, ttl);
    assert client.get(key).orElse(null).equals(value);
  }

  private void checkGetKeyTTL(ByteString key, long ttl) {
    Optional<Long> t = client.getKeyTTL(key);
    assertTrue(t.isPresent());
    assertTrue(t.get() <= ttl && t.get() > 0);
  }

  private void checkGetTTLTimeOut(ByteString key) {
    assertFalse(client.get(key).isPresent());
  }

  private void checkGetKeyTTLTimeOut(ByteString key) {
    Optional<Long> t = client.getKeyTTL(key);
    assertFalse(t.isPresent());
  }

  private void checkNotExist(ByteString key) {
    assertFalse(client.get(key).isPresent());
  }

  private static ByteString rawKey(String key) {
    return ByteString.copyFromUtf8(RAW_PREFIX + key);
  }

  private static ByteString rawValue(String value) {
    return ByteString.copyFromUtf8(value);
  }

  private static class ByteStringComparator implements Comparator<ByteString> {

    @Override
    public int compare(ByteString startKey, ByteString endKey) {
      return FastByteComparisons.compareTo(startKey.toByteArray(), endKey.toByteArray());
    }
  }
}
