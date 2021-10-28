package org.tikv;

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
import org.tikv.common.util.ScanOption;
import org.tikv.raw.RawKVClient;

/*
172.16.4.75
172.16.4.76
172.16.4.77
java -Dtikv.grpc.timeout_in_ms=150ms -Dtikv.grpc.forward_timeout_in_ms=200ms -cp tikv-client-java-3.1.4-SNAPSHOT.jar org.tikv.TimeoutTest all 10

chaosd attack network delay -d em1 -i 172.16.5.82 -l 500ms
chaosd attack network delay -d em1 -i 172.16.5.82 -l 250ms
chaosd attack network delay -d em1 -i 172.16.5.82 -l 100ms
chaosd attack network delay -d em1 -i 172.16.5.82 -l 50ms
chaosd attack network delay -d em1 -i 172.16.5.82 -l 0ms
 */
public class TimeoutTest {

  public static int TEST_ROUND = 10;
  public static Random random = new Random();

  public static String TEST_FUNC = "testGet";

  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      TEST_FUNC = args[0];
    }
    System.out.println("TEST_FUNC = " + TEST_FUNC);

    if (args.length > 1) {
      TEST_ROUND = Integer.parseInt(args[1]);
    }
    System.out.println("TEST_ROUND = " + TEST_ROUND);

    TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    long start = System.currentTimeMillis();
    if (!TEST_FUNC.equals("all")) {
      doTest(client);
    } else {
      List<String> functionList = new ArrayList<>();
      functionList.add("testGet");
      functionList.add("testPut");
      functionList.add("testBatchGet");
      functionList.add("testBatchPut");
      functionList.add("testScan");
      functionList.add("testScanPrefix");
      functionList.add("testBatchScan");
      functionList.add("testDelete");
      functionList.add("testBatchDelete");
      for (String func : functionList) {
        TEST_FUNC = func;
        doTest(client);
      }
    }

    long end = System.currentTimeMillis();
    System.out.println("total time = " + (end - start) / 1000 + "s");

    client.close();
    session.close();
  }

  public static ByteString genKey() {
    long index = Math.abs(random.nextLong() % 10000000000000L);
    return ByteString.copyFromUtf8(KEY_PREFIX + index);
  }

  public static void doTest(RawKVClient client) {
    ArrayList<Result> resultList = new ArrayList<>(TEST_ROUND);
    long failureCount = 0;
    System.out.println("======================");
    System.out.println("start test:" + TEST_FUNC);
    for (int i = 0; i < TEST_ROUND; i++) {
      System.out.println("start test round: " + i);
      long start = System.currentTimeMillis();
      Exception err = null;
      try {
        if (TEST_FUNC.equals("testGet")) {
          testGet(client);
        }

        if (TEST_FUNC.equals("testPut")) {
          testPut(client);
        }

        if (TEST_FUNC.equals("testBatchGet")) {
          testBatchGet(client);
        }

        if (TEST_FUNC.equals("testBatchPut")) {
          testBatchPut(client);
        }

        if (TEST_FUNC.equals("testScan")) {
          testScan(client);
        }

        if (TEST_FUNC.equals("testScanPrefix")) {
          testScanPrefix(client);
        }

        if (TEST_FUNC.equals("testBatchScan")) {
          testBatchScan(client);
        }

        if (TEST_FUNC.equals("testDelete")) {
          testDelete(client);
        }

        if (TEST_FUNC.equals("testBatchDelete")) {
          testBatchDelete(client);
        }

        /*if(TEST_FUNC.equals("testDeleteRange")) {
          testDeleteRange(client);
        }

        if(TEST_FUNC.equals("testDeletePrefix")) {
          testDeletePrefix(client);
        }*/
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
    ByteString key = genKey();
    String value = client.get(key).toStringUtf8();
    System.out.println(value);
  }

  // put
  static void testPut(RawKVClient client) {
    ByteString key = genKey();
    ByteString value = ByteString.copyFromUtf8("testPut");
    client.put(key, value);
  }

  // delete
  static void testDelete(RawKVClient client) {
    ByteString key = genKey();
    client.delete(key);
  }

  // batch get
  static void testBatchGet(RawKVClient client) {
    List<ByteString> keys = new ArrayList<>(50);
    for (int i = 0; i < 50; i++) {
      ByteString key = genKey();
      keys.add(key);
    }
    client.batchGet(keys);
  }

  // batch put
  static void testBatchPut(RawKVClient client) {
    Map<ByteString, ByteString> kvPairs = new HashMap<>(50);
    for (int i = 0; i < 50; i++) {
      ByteString key = genKey();
      ByteString value = ByteString.copyFromUtf8("testBatchPut");
      kvPairs.put(key, value);
    }
    client.batchPut(kvPairs);
  }

  // batch delete
  static void testBatchDelete(RawKVClient client) {
    List<ByteString> keys = new ArrayList<>(50);
    for (int i = 0; i < 50; i++) {
      ByteString key = genKey();
      keys.add(key);
    }
    client.batchDelete(keys);
  }

  // scan
  static void testScan(RawKVClient client) {
    ByteString key1 = genKey();
    ByteString key2 = genKey();
    ByteString startKey;
    ByteString endKey;

    if (Key.toRawKey(key1).compareTo(Key.toRawKey(key2)) > 0) {
      startKey = key2;
      endKey = key1;
    } else {
      startKey = key1;
      endKey = key2;
    }

    client.scan(startKey, endKey, 1000);
  }

  // scan prefix
  static void testScanPrefix(RawKVClient client) {
    ByteString key = genKey();
    client.scanPrefix(key);
  }

  // batchscan
  static void testBatchScan(RawKVClient client) {
    List<ScanOption> ranges = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      ByteString key1 = genKey();
      ByteString key2 = genKey();
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
    ByteString key1 = genKey();
    ByteString key2 = genKey();
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
    ByteString key = genKey();
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
    return "Result{" + "duration=" + duration + ", round=" + round + ", success=" + success + '}';
  }
}
