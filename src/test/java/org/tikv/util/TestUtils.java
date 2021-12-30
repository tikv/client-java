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

package org.tikv.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

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
