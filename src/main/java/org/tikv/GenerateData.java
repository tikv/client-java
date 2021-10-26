package org.tikv;

import com.google.protobuf.ByteString;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;

public class GenerateData {
  public static String PD_ADDRESS = "172.16.4.77:4689";

  public static String KEY_PREFIX = "rawkv_test_";
  public static String VALUE = "value";
  public static int KEY_COUNT = 1000;

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    generateData();
    long end = System.currentTimeMillis();
    System.out.println("total time = " + (end - start) / 1000 + "s");
  }

  public static void generateData() throws Exception {
    TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    long failureCount = 0;
    for (int i = 0; i < KEY_COUNT; i++) {
      ByteString k = ByteString.copyFromUtf8(KEY_PREFIX + i);
      ByteString v = ByteString.copyFromUtf8(VALUE);
      try {
        client.put(k, v);
      } catch (Exception e) {
        failureCount += 1;
        e.printStackTrace();
      } finally {
        System.out.println(i);
      }
    }
    System.out.println("failureCount = " + failureCount);
    session.close();
  }
}
