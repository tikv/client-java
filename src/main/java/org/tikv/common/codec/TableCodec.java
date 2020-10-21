/*
 * Copyright 2020 PingCAP, Inc.
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

import java.util.List;
import org.tikv.common.exception.CodecException;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.row.Row;

public class TableCodec {
  public static byte[] encodeRow(
      List<TiColumnInfo> columnInfos,
      Object[] values,
      boolean isPkHandle,
      boolean encodeWithNewRowFormat)
      throws IllegalAccessException {
    if (columnInfos.size() != values.length) {
      throw new IllegalAccessException(
          String.format(
              "encodeRow error: data and columnID count not " + "match %d vs %d",
              columnInfos.size(), values.length));
    }
    if (encodeWithNewRowFormat) {
      return TableCodecV2.encodeRow(columnInfos, values, isPkHandle);
    }
    return TableCodecV1.encodeRow(columnInfos, values, isPkHandle);
  }

  public static Row decodeRow(byte[] value, Long handle, TiTableInfo tableInfo) {
    if (value.length == 0) {
      throw new CodecException("Decode fails: value length is zero");
    }
    if ((value[0] & 0xff) == org.tikv.common.codec.RowV2.CODEC_VER) {
      return TableCodecV2.decodeRow(value, handle, tableInfo);
    }
    return TableCodecV1.decodeRow(value, handle, tableInfo);
  }

  public static long decodeHandle(byte[] value) {
    return new CodecDataInput(value).readLong();
  }
}
