package org.tikv.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtils {
  public static byte[] genRandomKey(String keyPrefix, int keyLength) {
    int length = keyLength - keyPrefix.length();
    if (length <= 0) {
      length = 0;
    }
    return (keyPrefix + genRandomString(length)).getBytes();
  }

  public static byte[] genRandomValue(int length) {
    return genRandomString(length).getBytes();
  }

  private static String genRandomString(int length) {
    Random rnd = ThreadLocalRandom.current();
    StringBuilder ret = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      boolean isChar = (rnd.nextInt(2) % 2 == 0);
      if (isChar) {
        int choice = rnd.nextInt(2) % 2 == 0 ? 65 : 97;
        ret.append((char) (choice + rnd.nextInt(26)));
      } else {
        ret.append(rnd.nextInt(10));
      }
    }
    return ret.toString();
  }
}
