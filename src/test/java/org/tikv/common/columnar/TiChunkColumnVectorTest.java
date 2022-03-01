/*
 * Copyright 2022 TiKV Project Authors.
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

package org.tikv.common.columnar;

import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.types.BitType;

public class TiChunkColumnVectorTest extends TestCase {

  @Test
  public void testGetLong() {
    long expect = 32767;
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(expect);
    TiChunkColumnVector tiChunkColumnVector =
        new TiChunkColumnVector(BitType.BIT, -1, 1, 0, new byte[] {-1}, new long[] {0, 8}, buffer);
    Assert.assertEquals(expect, tiChunkColumnVector.getLong(0));
  }
}
