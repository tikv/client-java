/*
 *
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
 *
 */

package org.tikv.common.types;

import java.util.Base64;
import org.tikv.common.codec.Codec;
import org.tikv.common.codec.Codec.IntegerCodec;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.exception.CastingException;
import org.tikv.common.exception.ConvertNotSupportException;
import org.tikv.common.exception.ConvertOverflowException;
import org.tikv.common.exception.TypeException;
import org.tikv.common.meta.TiColumnInfo;

public class BitType extends IntegerType {
  public static final BitType BIT = new BitType(MySQLType.TypeBit);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeBit};
  private static final long MAX_BIT_LENGTH = 64L;

  private BitType(MySQLType tp) {
    super(tp);
  }

  protected BitType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public long getSize() {
    if (isLengthUnSpecified()) {
      return getPrefixSize() + getDefaultDataSize();
    } else {
      return getPrefixSize() + (getLength() + 7) / 8;
    }
  }

  public String getName() {
    return "BIT";
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    Long result = Converter.safeConvertToUnsigned(value, this.unsignedUpperBound());
    long targetLength = this.getLength();
    long upperBound = 1L << targetLength;
    if (targetLength < MAX_BIT_LENGTH && Long.compareUnsigned(result, upperBound) >= 0) {
      throw ConvertOverflowException.newUpperBoundException(result, upperBound);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    switch (flag) {
      case Codec.UVARINT_FLAG:
        return IntegerCodec.readUVarLong(cdi);
      case Codec.UINT_FLAG:
        return IntegerCodec.readULong(cdi);
      default:
        throw new TypeException("Invalid IntegerType flag: " + flag);
    }
  }

  @Override
  public boolean isUnsigned() {
    return true;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    // Default value use to stored in DefaultValue field, but now, bit type default value will store
    // in DefaultValueBit for fix bit default value decode/encode bug.
    // DefaultValueBit is encoded using Base64.
    Long result = 0L;
    byte[] bytes = Base64.getDecoder().decode(value);
    if (bytes.length <= 0 || bytes.length > 8) {
      throw new CastingException("Base64 format Bit Type to Long Overflow");
    }
    int size = bytes.length;
    for (int i = 0; i < bytes.length; i++) {
      result += ((long) (bytes[size - i - 1] & 0xff)) << ((size - i - 1) * 8);
    }
    return result;
  }

  @Override
  public boolean isPushDownSupported() {
    return false;
  }
}
