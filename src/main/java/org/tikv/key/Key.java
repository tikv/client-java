/*
 * Copyright 2017 PingCAP, Inc.
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

package org.tikv.key;

import static java.util.Objects.requireNonNull;
import static org.tikv.codec.KeyUtils.formatBytes;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.tikv.util.FastByteComparisons;

public class Key implements Comparable<Key> {

  protected final byte[] value;
  protected final int infFlag;

  public static final Key EMPTY = createEmpty();

  private Key(byte[] value, boolean negative) {
    this.value = requireNonNull(value, "value is null");
    this.infFlag = (value.length == 0 ? 1 : 0) * (negative ? -1 : 1);
  }

  protected Key(byte[] value) {
    this(value, false);
  }

  public static Key toRawKey(ByteString bytes, boolean negative) {
    return new Key(bytes.toByteArray(), negative);
  }

  public static Key toRawKey(ByteString bytes) {
    return new Key(bytes.toByteArray());
  }

  public static Key toRawKey(byte[] bytes, boolean negative) {
    return new Key(bytes, negative);
  }

  public static Key toRawKey(byte[] bytes) {
    return new Key(bytes);
  }

  private static Key createEmpty() {
    return new Key(new byte[0]) {
      @Override
      public Key next() {
        return this;
      }

      @Override
      public String toString() {
        return "EMPTY";
      }
    };
  }

  /**
   * The next key for bytes domain It first plus one at LSB and if LSB overflows, a zero byte is
   * appended at the end Original bytes will be reused if possible
   *
   * @return encoded results
   */
  public Key next() {
    return toRawKey(nextValue(value));
  }

  static byte[] nextValue(byte[] value) {
    int i;
    byte[] newVal = Arrays.copyOf(value, value.length);
    for (i = newVal.length - 1; i >= 0; i--) {
      newVal[i]++;
      if (newVal[i] != 0) {
        break;
      }
    }
    if (i == -1) {
      return Arrays.copyOf(value, value.length + 1);
    } else {
      return newVal;
    }
  }

  @Override
  public int compareTo(Key other) {
    requireNonNull(other, "other is null");
    if ((this.infFlag | other.infFlag) != 0) {
      return this.infFlag - other.infFlag;
    }
    return FastByteComparisons.compareTo(value, other.value);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof Key) {
      return compareTo((Key) other) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value) * infFlag;
  }

  public byte[] getBytes() {
    return value;
  }

  public ByteString toByteString() {
    return ByteString.copyFrom(value);
  }

  public int getInfFlag() {
    return infFlag;
  }

  @Override
  public String toString() {
    if (infFlag < 0) {
      return "-INF";
    } else if (infFlag > 0) {
      return "+INF";
    } else {
      return String.format("{%s}", formatBytes(value));
    }
  }
}
