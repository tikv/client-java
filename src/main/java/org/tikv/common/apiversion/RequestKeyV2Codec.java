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

package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Region;

public class RequestKeyV2Codec implements RequestKeyCodec {
  protected static final ByteString RAW_DEFAULT_PREFIX =
      ByteString.copyFrom(new byte[] {'r', 0, 0, 0});
  protected static final ByteString RAW_DEFAULT_END =
      ByteString.copyFrom(new byte[] {'r', 0, 0, 1});
  protected static final ByteString TXN_DEFAULT_PREFIX =
      ByteString.copyFrom(new byte[] {'x', 0, 0, 0});
  protected static final ByteString TXN_DEFAULT_END =
      ByteString.copyFrom(new byte[] {'x', 0, 0, 1});
  protected ByteString keyPrefix;
  protected ByteString infiniteEndKey;

  @Override
  public ByteString encodeKey(ByteString key) {
    return keyPrefix.concat(key);
  }

  @Override
  public ByteString decodeKey(ByteString key) {
    if (key.isEmpty()) {
      return key;
    }

    if (!key.startsWith(keyPrefix)) {
      throw new IllegalArgumentException("key corrupted, wrong prefix");
    }

    return key.substring(keyPrefix.size());
  }

  @Override
  public Pair<ByteString, ByteString> encodeRange(ByteString start, ByteString end) {
    start = encodeKey(start);

    end = end.isEmpty() ? infiniteEndKey : encodeKey(end);

    return Pair.create(start, end);
  }

  @Override
  public ByteString encodePdQuery(ByteString key) {
    return CodecUtils.encode(encodeKey(key));
  }

  @Override
  public Pair<ByteString, ByteString> encodePdQueryRange(ByteString start, ByteString end) {
    Pair<ByteString, ByteString> range = encodeRange(start, end);
    return Pair.create(CodecUtils.encode(range.first), CodecUtils.encode(range.second));
  }

  @Override
  public Region decodeRegion(Region region) {
    Metapb.Region.Builder builder = Metapb.Region.newBuilder().mergeFrom(region);

    ByteString start = region.getStartKey();
    ByteString end = region.getEndKey();

    if (!start.isEmpty()) {
      start = CodecUtils.decode(start);
    }

    if (!end.isEmpty()) {
      end = CodecUtils.decode(end);
    }

    if (ByteString.unsignedLexicographicalComparator().compare(start, infiniteEndKey) >= 0
        || (!end.isEmpty()
            && ByteString.unsignedLexicographicalComparator().compare(end, keyPrefix) <= 0)) {
      throw new IllegalArgumentException("region out of keyspace" + region.toString());
    }

    start = start.startsWith(keyPrefix) ? start.substring(keyPrefix.size()) : ByteString.EMPTY;
    end = end.startsWith(keyPrefix) ? end.substring(keyPrefix.size()) : ByteString.EMPTY;

    return builder.setStartKey(start).setEndKey(end).build();
  }
}
