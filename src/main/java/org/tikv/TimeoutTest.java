package org.tikv;

import static org.tikv.GenerateData.KEY_COUNT;
import static org.tikv.GenerateData.KEY_PREFIX;
import static org.tikv.GenerateData.PD_ADDRESS;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.policy.RetryPolicy;
import org.tikv.common.util.ScanOption;
import org.tikv.raw.RawKVClient;

public class TimeoutTest {

  public static int TEST_ROUND = 10;
  public static int PD_ERROR_RATE = 50;
  public static int TIKV_ERROR_RATE = 50;

  public static void main(String[] args) throws Exception {
    RetryPolicy.pdErrorRate = PD_ERROR_RATE;
    RetryPolicy.tikvErrorRate = TIKV_ERROR_RATE;
    TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    long start = System.currentTimeMillis();
    doTest(client);
    long end = System.currentTimeMillis();
    System.out.println("total time = " + (end - start) / 1000 + "s");

    client.close();
    session.close();
  }

  public static void doTest(RawKVClient client) {
    ArrayList<Result> resultList = new ArrayList<>(TEST_ROUND);
    long failureCount = 0;
    for (int i = 0; i < TEST_ROUND; i++) {
      System.out.println("start test round: " + i);
      long start = System.currentTimeMillis();
      Exception err = null;
      try {
        //testGet(client);
        //testPut(client);
        //testBatchGet(client);
        //testBatchPut(client);
        //testScan(client);
        //testScanPrefix(client);
        //testBatchScan(client);

        //testDelete(client);
        //testBatchDelete(client);
        //testDeleteRange(client);
        //testDeletePrefix(client);
      } catch (Exception e) {
        e.printStackTrace();
        failureCount += 1;
        err = e;
      } finally {
        long end = System.currentTimeMillis();
        long duration = end - start;
        Result r = new Result(duration, i, err == null);
        resultList.add(r);
        System.out.println(r);
        System.out.println("finish test round: " + i);
      }
    }
    resultList.sort(Comparator.comparing(o -> o.duration));
    for (Result r : resultList) {
      System.out.println(r);
    }
    System.out.println("failureCount = " + failureCount);
  }

  // get
  static void testGet(RawKVClient client) {
    int keyNumber = new Random().nextInt(KEY_COUNT);
    ByteString key = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber);
    client.get(key).toStringUtf8();
  }

  // put
  static void testPut(RawKVClient client) {
    int keyNumber = new Random().nextInt(KEY_COUNT);
    ByteString key = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber);
    ByteString value = ByteString.copyFromUtf8("testPut");
    client.put(key, value);
  }

  // delete
  static void testDelete(RawKVClient client) {
    int keyNumber = new Random().nextInt(KEY_COUNT);
    ByteString key = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber);
    client.delete(key);
  }

  // batch get TODO: mars
  static void testBatchGet(RawKVClient client) {
    List<ByteString> keys = new ArrayList<>(50);
    for (int i = 0; i < 50; i++) {
      int keyNumber = new Random().nextInt(KEY_COUNT);
      ByteString key = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber);
      keys.add(key);
    }
    client.batchGet(keys);
  }

  // batch put TODO: mars
  static void testBatchPut(RawKVClient client) {
    Map<ByteString, ByteString> kvPairs = new HashMap<>(50);
    for (int i = 0; i < 50; i++) {
      int keyNumber = new Random().nextInt(KEY_COUNT);
      ByteString key = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber);
      ByteString value = ByteString.copyFromUtf8("testBatchPut");
      kvPairs.put(key, value);
    }
    client.batchPut(kvPairs);
  }

  // batch delete
  static void testBatchDelete(RawKVClient client) {
    List<ByteString> keys = new ArrayList<>(50);
    for (int i = 0; i < 50; i++) {
      int keyNumber = new Random().nextInt(KEY_COUNT);
      ByteString key = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber);
      keys.add(key);
    }
    client.batchDelete(keys);
  }

  // scan
  static void testScan(RawKVClient client) {
    int keyNumber1 = new Random().nextInt(KEY_COUNT);
    int keyNumber2 = new Random().nextInt(KEY_COUNT);
    ByteString key1 = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber1);
    ByteString key2 = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber2);
    ByteString startKey;
    ByteString endKey;

    if (Key.toRawKey(key1).compareTo(Key.toRawKey(key2)) > 0) {
      startKey = key2;
      endKey = key1;
    } else {
      startKey = key1;
      endKey = key2;
    }

    client.scan(startKey, endKey);
  }

  // scan prefix
  static void testScanPrefix(RawKVClient client) {
    ByteString key = ByteString.copyFromUtf8(KEY_PREFIX);
    client.scanPrefix(key);
  }

  // batchscan: error ???????????
  static void testBatchScan(RawKVClient client) {
    List<ScanOption> ranges = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      int keyNumber1 = new Random().nextInt(KEY_COUNT);
      int keyNumber2 = new Random().nextInt(KEY_COUNT);
      ByteString key1 = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber1);
      ByteString key2 = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber2);
      ByteString startKey;
      ByteString endKey;

      if (Key.toRawKey(key1).compareTo(Key.toRawKey(key2)) > 0) {
        startKey = key2;
        endKey = key1;
      } else {
        startKey = key1;
        endKey = key2;
      }
      ScanOption opt =
          ScanOption.newBuilder().setStartKey(startKey).setEndKey(endKey).setLimit(50).build();
      ranges.add(opt);
    }
    client.batchScan(ranges);
  }

  // delete range
  static void testDeleteRange(RawKVClient client) {
    int keyNumber1 = new Random().nextInt(KEY_COUNT);
    int keyNumber2 = new Random().nextInt(KEY_COUNT);
    ByteString key1 = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber1);
    ByteString key2 = ByteString.copyFromUtf8(KEY_PREFIX + keyNumber2);
    ByteString startKey;
    ByteString endKey;

    if (Key.toRawKey(key1).compareTo(Key.toRawKey(key2)) > 0) {
      startKey = key2;
      endKey = key1;
    } else {
      startKey = key1;
      endKey = key2;
    }
    client.deleteRange(startKey, endKey);
  }

  // delete prefix
  static void testDeletePrefix(RawKVClient client) {
    ByteString key = ByteString.copyFromUtf8(KEY_PREFIX);
    client.deletePrefix(key);
  }
}

class Result {
  public Long duration;
  public int round;
  public boolean success;

  public Result(long duration, int round, boolean success) {
    this.duration = duration;
    this.round = round;
    this.success = success;
  }

  @Override
  public String toString() {
    return "Result{" +
        "duration=" + duration +
        ", round=" + round +
        ", success=" + success +
        '}';
  }
}
