package org.tikv.common.handle;

import java.util.Arrays;
import org.tikv.common.codec.Codec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.exception.CodecException;
import org.tikv.common.types.DataType;
import org.tikv.common.types.IntegerType;

public class IntHandle implements Handle {
  private final long handle;
  private final int infFlag;

  public IntHandle(long handle) {
    this.handle = handle;
    this.infFlag = 0;
  }

  private IntHandle(long handle, int infFlag) {
    this.handle = handle;
    this.infFlag = infFlag;
  }

  @Override
  public boolean isInt() {
    return true;
  }

  @Override
  public long intValue() {
    return handle;
  }

  @Override
  public Handle next() {
    if (handle != Long.MAX_VALUE) {
      return new IntHandle(handle + 1);
    }
    return new IntHandle(handle, 1);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof IntHandle) {
      return ((IntHandle) other).intValue() == handle;
    }
    return false;
  }

  @Override
  public int compare(Handle h) {
    if (!h.isInt()) {
      throw new RuntimeException("IntHandle compares to CommonHandle");
    }
    if (infFlag != ((IntHandle) h).infFlag) {
      return infFlag - ((IntHandle) h).infFlag;
    }
    long val = intValue();
    long hVal = h.intValue();
    if (val > hVal) {
      return 1;
    } else if (val < hVal) {
      return -1;
    }
    return 0;
  }

  @Override
  public byte[] encoded() {
    CodecDataOutput cdo = new CodecDataOutput();
    Codec.IntegerCodec.writeLong(cdo, handle);
    byte[] encoded = cdo.toBytes();
    if (infFlag == 1) {
      return Arrays.copyOf(encoded, encoded.length + 1);
    }
    return encoded;
  }

  @Override
  public byte[] encodedAsKey() {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerType.BIGINT.encode(cdo, DataType.EncodeType.KEY, handle);
    byte[] encoded = cdo.toBytes();
    if (infFlag == 1) {
      return Arrays.copyOf(encoded, encoded.length + 1);
    }
    return encoded;
  }

  @Override
  public int len() {
    if (infFlag == 1) {
      return 9;
    }
    return 8;
  }

  @Override
  public int numCols() {
    throw new CodecException("not supported in IntHandle");
  }

  @Override
  public byte[] encodedCol(int idx) {
    throw new CodecException("not supported in IntHandle");
  }

  @Override
  public Object[] data() {
    return new Object[] {handle};
  }

  @Override
  public String toString() {
    if (infFlag == -1) {
      return "-inf";
    } else if (infFlag == 1) {
      return "+inf";
    }
    return String.valueOf(handle);
  }
}
