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

package org.tikv.common.key;

import static org.junit.Assert.assertEquals;
import static org.tikv.common.key.Key.toRawKey;

import com.google.common.primitives.UnsignedBytes;
import org.junit.Test;

public class KeyTest {

  @Test
  public void nextTest() throws Exception {
    Key k1 = toRawKey(new byte[] {1, 2, 3});
    assertEquals(toRawKey(new byte[] {1, 2, 4}), k1.next());

    k1 = toRawKey(new byte[] {UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE});
    assertEquals(
        toRawKey(new byte[] {UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE, 0}), k1.next());
  }
}
