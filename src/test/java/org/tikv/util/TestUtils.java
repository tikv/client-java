package org.tikv.util;

public class TestUtils {
  public static String getEnv(String key) {
    String tmp = System.getenv(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    tmp = System.getProperty(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    return null;
  }
}
