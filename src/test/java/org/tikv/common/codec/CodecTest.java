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

package org.tikv.common.codec;

import static org.junit.Assert.*;
import static org.tikv.common.codec.Codec.*;

import org.junit.Test;
import org.tikv.common.codec.Codec.*;

public class CodecTest {
  @Test
  public void readNWriteLongTest() {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeLongFully(cdo, 9999L, true);
    IntegerCodec.writeLongFully(cdo, -2333L, false);
    assertArrayEquals(
        new byte[] {
          (byte) 0x3,
          (byte) 0x80,
          (byte) 0x0,
          (byte) 0x0,
          (byte) 0x0,
          (byte) 0x0,
          (byte) 0x0,
          (byte) 0x27,
          (byte) 0xf,
          (byte) 0x8,
          (byte) 0xb9,
          (byte) 0x24
        },
        cdo.toBytes());
    CodecDataInput cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(INT_FLAG, cdi.readByte());
    long value = IntegerCodec.readLong(cdi);
    assertEquals(9999L, value);
    assertEquals(VARINT_FLAG, cdi.readByte());
    value = IntegerCodec.readVarLong(cdi);
    assertEquals(-2333L, value);

    byte[] wrongData = new byte[] {(byte) 0x8, (byte) 0xb9};
    cdi = new CodecDataInput(wrongData);
    try {
      IntegerCodec.readLong(cdi);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void readNWriteUnsignedLongTest() {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeULongFully(cdo, 0xffffffffffffffffL, true);
    IntegerCodec.writeULongFully(cdo, Long.MIN_VALUE, false);
    assertArrayEquals(
        new byte[] {
          (byte) 0x4,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0xff,
          (byte) 0x9,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x80,
          (byte) 0x1
        },
        cdo.toBytes());
    CodecDataInput cdi = new CodecDataInput(cdo.toBytes());

    assertEquals(UINT_FLAG, cdi.readByte());
    long value = IntegerCodec.readULong(cdi);
    assertEquals(0xffffffffffffffffL, value);

    assertEquals(UVARINT_FLAG, cdi.readByte());
    value = IntegerCodec.readUVarLong(cdi);
    assertEquals(Long.MIN_VALUE, value);

    byte[] wrongData =
        new byte[] {
          (byte) 0x9, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80
        };
    cdi = new CodecDataInput(wrongData);
    try {
      cdi.skipBytes(1);
      IntegerCodec.readUVarLong(cdi);
      fail();
    } catch (Exception e) {
      assertEquals("readUVarLong encountered unfinished data", e.getMessage());
    }

    // the following two tests are for overflow readUVarLong
    wrongData =
        new byte[] {
          (byte) 0x9, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80, (byte) 0x02
        };
    cdi = new CodecDataInput(wrongData);
    try {
      cdi.skipBytes(1);
      IntegerCodec.readUVarLong(cdi);
      fail();
    } catch (Exception e) {
      assertEquals("readUVarLong overflow", e.getMessage());
    }

    wrongData =
        new byte[] {
          (byte) 0x9, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01
        };
    cdi = new CodecDataInput(wrongData);
    try {
      cdi.skipBytes(1);
      IntegerCodec.readUVarLong(cdi);
      fail();
    } catch (Exception e) {
      assertEquals("readUVarLong overflow", e.getMessage());
    }
  }

  private static byte[] toBytes(int[] arr) {
    byte[] bytes = new byte[arr.length];
    for (int i = 0; i < arr.length; i++) {
      bytes[i] = (byte) (arr[i] & 0xFF);
    }
    return bytes;
  }

  @Test
  public void writeBytesTest() {
    CodecDataOutput cdo = new CodecDataOutput();
    Codec.BytesCodec.writeBytes(cdo, "abcdefghijk".getBytes());
    byte[] result = cdo.toBytes();
    byte[] expected =
        toBytes(
            new int[] {
              97, 98, 99, 100, 101, 102, 103, 104, 255, 105, 106, 107, 0, 0, 0, 0, 0, 250
            });
    assertArrayEquals(expected, result);

    cdo.reset();
    Codec.BytesCodec.writeBytes(cdo, "abcdefghijk".getBytes());
    result = BytesCodec.readBytes(new CodecDataInput(cdo.toBytes()));
    expected = toBytes(new int[] {97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107});
    assertArrayEquals(expected, result);

    cdo.reset();
    Codec.BytesCodec.writeBytes(cdo, "fYfSp".getBytes());
    result = cdo.toBytes();
    expected = toBytes(new int[] {102, 89, 102, 83, 112, 0, 0, 0, 252});
    assertArrayEquals(expected, result);

    cdo.reset();
    Codec.BytesCodec.writeBytesRaw(cdo, "fYfSp".getBytes());
    result = cdo.toBytes();
    expected = toBytes(new int[] {102, 89, 102, 83, 112});
    assertArrayEquals(expected, result);
  }

  @Test
  public void readBytesTest() {
    // TODO: How to test private
    byte[] data =
        new byte[] {
          (byte) 0x61,
          (byte) 0x62,
          (byte) 0x63,
          (byte) 0x64,
          (byte) 0x65,
          (byte) 0x66,
          (byte) 0x67,
          (byte) 0x68,
          (byte) 0xff,
          (byte) 0x69,
          (byte) 0x6a,
          (byte) 0x6b,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0xfa
        };
    CodecDataInput cdi = new CodecDataInput(data);
    // byte[] result = BytesCodec.readBytes(cdi, false);

  }
}
