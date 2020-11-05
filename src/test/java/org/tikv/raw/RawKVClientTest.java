package org.tikv.raw;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.kvproto.Kvrpcpb;

public class RawKVClientTest {
  private static final String DEFAULT_PD_ADDRESS = "127.0.0.1:2379";
  private static final String RAW_PREFIX = "raw_\u0001_";
  private static final int KEY_POOL_SIZE = 1000000;
  private static final int TEST_CASES = 10000;
  private static final int WORKER_CNT = 100;
  private static final ByteString RAW_START_KEY = ByteString.copyFromUtf8(RAW_PREFIX);
  private static final ByteString RAW_END_KEY = Key.toRawKey(RAW_START_KEY).next().toByteString();
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
  public void setup() throws IOException {
    try {
      TiConfiguration conf = TiConfiguration.createRawDefault(DEFAULT_PD_ADDRESS);
      conf.setFollowerRead(true);
      session = TiSession.create(conf);
      initialized = false;
      if (client == null) {
        client = session.createRawClient();
      }
      data = new TreeMap<>(bsc);
      initialized = true;
    } catch (Exception e) {
      logger.warn("Cannot initialize raw client. Test skipped.", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void simpleTest() {
    if (!initialized) return;
    ByteString key = rawKey("key");
    ByteString key1 = rawKey("key1");
    ByteString key2 = rawKey("key2");
    ByteString key3 = rawKey("key3");
    ByteString value1 = rawValue("value1");
    ByteString value2 = rawValue("value2");
    Kvrpcpb.KvPair kv1 = Kvrpcpb.KvPair.newBuilder().setKey(key1).setValue(value1).build();
    Kvrpcpb.KvPair kv2 = Kvrpcpb.KvPair.newBuilder().setKey(key2).setValue(value2).build();

    try {
      checkEmpty(key1);
      checkEmpty(key2);
      checkPut(key1, value1);
      checkPut(key2, value2);
      List<Kvrpcpb.KvPair> result = new ArrayList<>();
      List<Kvrpcpb.KvPair> result2 = new ArrayList<>();
      result.add(kv1);
      result.add(kv2);
      checkScan(key, key3, result, limit);
      checkScan(key1, key3, result, limit);
      result2.add(kv1);
      checkScan(key, key2, result2, limit);
      checkDelete(key1);
      checkDelete(key2);
    } catch (final TiKVException e) {
      logger.warn("Test fails with Exception: " + e);
    }
  }

  private List<Kvrpcpb.KvPair> rawKeys() {
    return client.scan(RAW_START_KEY, RAW_END_KEY, limit);
  }

  @Test
  public void validate() {
    if (!initialized) return;
    baseTest(100, 100, 100, 100, false, false);
    baseTest(100, 100, 100, 100, false, true);
  }

  /** Example of benchmarking base test */
  public void benchmark() {
    if (!initialized) return;
    baseTest(TEST_CASES, TEST_CASES, 200, 5000, true, false);
    baseTest(TEST_CASES, TEST_CASES, 200, 5000, true, true);
  }

  private void baseTest(
      int putCases,
      int getCases,
      int scanCases,
      int deleteCases,
      boolean benchmark,
      boolean batchPut) {
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
      rawGetTest(getCases, benchmark);
      rawScanTest(scanCases, benchmark);
      rawDeleteTest(deleteCases, benchmark);

      prepare();
    } catch (final TiKVException e) {
      logger.warn("Test fails with Exception " + e);
    }
    logger.info("ok, test done");
  }

  private void prepare() {
    logger.info("Initializing test");
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
            for (int j = 0; j < base; j++) checkDelete(remainingKeys.get(i * base + j).getKey());
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
        assert client.get(pair.getKey()).equals(pair.getValue());
        i++;
        if (i >= getCases) {
          break;
        }
      }
    }
  }

  private void rawScanTest(int scanCases, boolean benchmark) {
    logger.info("rawScan testing");
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

  private void checkPut(ByteString key, ByteString value) {
    client.put(key, value);
    assert client.get(key).equals(value);
  }

  private void checkBatchPut(Map<ByteString, ByteString> kvPairs) {
    client.batchPut(kvPairs);
    for (Map.Entry<ByteString, ByteString> kvPair : kvPairs.entrySet()) {
      assert client.get(kvPair.getKey()).equals(kvPair.getValue());
    }
  }

  private void checkScan(
      ByteString startKey, ByteString endKey, List<Kvrpcpb.KvPair> ans, int limit) {
    List<Kvrpcpb.KvPair> result = client.scan(startKey, endKey, limit);
    assert result.equals(ans);
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

  private void checkDelete(ByteString key) {
    client.delete(key);
    checkEmpty(key);
  }

  private void checkEmpty(ByteString key) {
    assert client.get(key).isEmpty();
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
