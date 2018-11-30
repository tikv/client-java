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

package org.tikv.codec;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import org.tikv.exception.InvalidCodecFormatException;

public class Codec {

  public static final int NULL_FLAG = 0;
  public static final int BYTES_FLAG = 1;
  public static final int COMPACT_BYTES_FLAG = 2;
  public static final int INT_FLAG = 3;
  public static final int UINT_FLAG = 4;
  public static final int FLOATING_FLAG = 5;
  public static final int DECIMAL_FLAG = 6;
  public static final int DURATION_FLAG = 7;
  public static final int VARINT_FLAG = 8;
  public static final int UVARINT_FLAG = 9;
  public static final int JSON_FLAG = 10;
  public static final int MAX_FLAG = 250;

  public static boolean isNullFlag(int flag) {
    return flag == NULL_FLAG;
  }

  public static class IntegerCodec {
    private static final long SIGN_MASK = ~Long.MAX_VALUE;

    private static long flipSignBit(long v) {
      return v ^ SIGN_MASK;
    }

    /**
     * Encoding a long value to byte buffer with type flag at the beginning
     *
     * @param cdo For outputting data in bytes array
     * @param lVal The data to encode
     * @param comparable If the output should be memory comparable without decoding. In real TiDB
     *     use case, if used in Key encoding, we output memory comparable format otherwise not
     */
    public static void writeLongFully(CodecDataOutput cdo, long lVal, boolean comparable) {
      if (comparable) {
        cdo.writeByte(INT_FLAG);
        writeLong(cdo, lVal);
      } else {
        cdo.writeByte(VARINT_FLAG);
        writeVarLong(cdo, lVal);
      }
    }

    /**
     * Encoding a unsigned long value to byte buffer with type flag at the beginning
     *
     * @param cdo For outputting data in bytes array
     * @param lVal The data to encode, note that long is treated as unsigned
     * @param comparable If the output should be memory comparable without decoding. In real TiDB
     *     use case, if used in Key encoding, we output memory comparable format otherwise not
     */
    public static void writeULongFully(CodecDataOutput cdo, long lVal, boolean comparable) {
      if (comparable) {
        cdo.writeByte(UINT_FLAG);
        writeULong(cdo, lVal);
      } else {
        cdo.writeByte(UVARINT_FLAG);
        writeUVarLong(cdo, lVal);
      }
    }

    /**
     * Encode long value without type flag at the beginning The signed bit is flipped for memory
     * comparable purpose
     *
     * @param cdo For outputting data in bytes array
     * @param lVal The data to encode
     */
    public static void writeLong(CodecDataOutput cdo, long lVal) {
      cdo.writeLong(flipSignBit(lVal));
    }

    /**
     * Encode long value without type flag at the beginning
     *
     * @param cdo For outputting data in bytes array
     * @param lVal The data to encode
     */
    public static void writeULong(CodecDataOutput cdo, long lVal) {
      cdo.writeLong(lVal);
    }

    /**
     * Encode var-length long, same as go's binary.PutVarint
     *
     * @param cdo For outputting data in bytes array
     * @param value The data to encode
     */
    static void writeVarLong(CodecDataOutput cdo, long value) {
      long ux = value << 1;
      if (value < 0) {
        ux = ~ux;
      }
      writeUVarLong(cdo, ux);
    }

    /**
     * Encode Data as var-length long, the same as go's binary.PutUvarint
     *
     * @param cdo For outputting data in bytes array
     * @param value The data to encode
     */
    static void writeUVarLong(CodecDataOutput cdo, long value) {
      while ((value - 0x80) >= 0) {
        cdo.writeByte((byte) value | 0x80);
        value >>>= 7;
      }
      cdo.writeByte((byte) value);
    }

    /**
     * Decode as signed long, assuming encoder flips signed bit for memory comparable
     *
     * @param cdi source of data
     * @return decoded signed long value
     */
    public static long readLong(CodecDataInput cdi) {
      return flipSignBit(cdi.readLong());
    }

    public static long readPartialLong(CodecDataInput cdi) {
      return flipSignBit(cdi.readPartialLong());
    }

    /**
     * Decode as unsigned long without any binary manipulation
     *
     * @param cdi source of data
     * @return decoded unsigned long value
     */
    public static long readULong(CodecDataInput cdi) {
      return cdi.readLong();
    }

    /**
     * Decode as var-length long, the same as go's binary.Varint
     *
     * @param cdi source of data
     * @return decoded signed long value
     */
    public static long readVarLong(CodecDataInput cdi) {
      long ux = readUVarLong(cdi);
      long x = ux >>> 1;
      if ((ux & 1) != 0) {
        x = ~x;
      }
      return x;
    }

    /**
     * Decode as var-length unsigned long, the same as go's binary.Uvarint
     *
     * @param cdi source of data
     * @return decoded unsigned long value
     */
    public static long readUVarLong(CodecDataInput cdi) {
      long x = 0;
      int s = 0;
      for (int i = 0; !cdi.eof(); i++) {
        long b = cdi.readUnsignedByte();
        if ((b - 0x80) < 0) {
          if (i > 9 || i == 9 && b > 1) {
            throw new InvalidCodecFormatException("readUVarLong overflow");
          }
          return x | b << s;
        }
        x |= (b & 0x7f) << s;
        s += 7;
      }
      throw new InvalidCodecFormatException("readUVarLong encountered unfinished data");
    }
  }

  public static class BytesCodec {

    private static final int GRP_SIZE = 8;
    private static final byte[] PADS = new byte[GRP_SIZE];
    private static final int MARKER = 0xFF;
    private static final byte PAD = (byte) 0x0;

    public static void writeBytesRaw(CodecDataOutput cdo, byte[] data) {
      cdo.write(data);
    }

    public static void writeBytesFully(CodecDataOutput cdo, byte[] data) {
      cdo.write(Codec.BYTES_FLAG);
      BytesCodec.writeBytes(cdo, data);
    }

    // writeBytes guarantees the encoded value is in ascending order for comparison,
    // encoding with the following rule:
    //  [group1][marker1]...[groupN][markerN]
    //  group is 8 bytes slice which is padding with 0.
    //  marker is `0xFF - padding 0 count`
    // For example:
    //   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
    //   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
    //   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
    //   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
    // Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    public static void writeBytes(CodecDataOutput cdo, byte[] data) {
      for (int i = 0; i <= data.length; i += GRP_SIZE) {
        int remain = data.length - i;
        int padCount = 0;
        if (remain >= GRP_SIZE) {
          cdo.write(data, i, GRP_SIZE);
        } else {
          padCount = GRP_SIZE - remain;
          cdo.write(data, i, data.length - i);
          cdo.write(PADS, 0, padCount);
        }
        cdo.write((byte) (MARKER - padCount));
      }
    }

    public static void writeCompactBytesFully(CodecDataOutput cdo, byte[] data) {
      cdo.write(Codec.COMPACT_BYTES_FLAG);
      writeCompactBytes(cdo, data);
    }

    /**
     * Write bytes in a compact form.
     *
     * @param cdo destination of data.
     * @param data is value that will be written into cdo.
     */
    static void writeCompactBytes(CodecDataOutput cdo, byte[] data) {
      int length = data.length;
      IntegerCodec.writeVarLong(cdo, length);
      cdo.write(data);
    }

    // readBytes decodes bytes which is encoded by EncodeBytes before,
    // returns the leftover bytes and decoded value if no error.
    public static byte[] readBytes(CodecDataInput cdi) {
      return readBytes(cdi, false);
    }

    public static byte[] readCompactBytes(CodecDataInput cdi) {
      int size = (int) IntegerCodec.readVarLong(cdi);
      return readCompactBytes(cdi, size);
    }

    private static byte[] readCompactBytes(CodecDataInput cdi, int size) {
      byte[] data = new byte[size];
      for (int i = 0; i < size; i++) {
        data[i] = cdi.readByte();
      }
      return data;
    }

    private static byte[] readBytes(CodecDataInput cdi, boolean reverse) {
      CodecDataOutput cdo = new CodecDataOutput();
      while (true) {
        byte[] groupBytes = new byte[GRP_SIZE + 1];

        cdi.readFully(groupBytes, 0, GRP_SIZE + 1);
        byte[] group = Arrays.copyOfRange(groupBytes, 0, GRP_SIZE);

        int padCount;
        int marker = Byte.toUnsignedInt(groupBytes[GRP_SIZE]);

        if (reverse) {
          padCount = marker;
        } else {
          padCount = MARKER - marker;
        }

        checkArgument(padCount <= GRP_SIZE);
        int realGroupSize = GRP_SIZE - padCount;
        cdo.write(group, 0, realGroupSize);

        if (padCount != 0) {
          byte padByte = PAD;
          if (reverse) {
            padByte = (byte) MARKER;
          }
          // Check validity of padding bytes.
          for (int i = realGroupSize; i < group.length; i++) {
            byte b = group[i];
            checkArgument(padByte == b);
          }
          break;
        }
      }
      byte[] bytes = cdo.toBytes();
      if (reverse) {
        for (int i = 0; i < bytes.length; i++) {
          bytes[i] = (byte) ~bytes[i];
        }
      }
      return bytes;
    }
  }
}
