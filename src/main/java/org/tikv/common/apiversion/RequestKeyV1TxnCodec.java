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
import org.apache.commons.lang3.tuple.Pair;
import org.tikv.kvproto.Metapb;

public class RequestKeyV1TxnCodec extends RequestKeyV1Codec implements RequestKeyCodec {
  public RequestKeyV1TxnCodec() {}

  @Override
  public ByteString encodePdQuery(ByteString key) {
    return CodecUtils.encode(key);
  }

  @Override
  public Pair<ByteString, ByteString> encodePdQueryRange(ByteString start, ByteString end) {
    if (!start.isEmpty()) {
      start = CodecUtils.encode(start);
    }

    if (!end.isEmpty()) {
      end = CodecUtils.encode(end);
    }

    return Pair.of(start, end);
  }

  @Override
  public Metapb.Region decodeRegion(Metapb.Region region) {
    Metapb.Region.Builder builder = Metapb.Region.newBuilder().mergeFrom(region);
    ByteString start = region.getStartKey();
    ByteString end = region.getEndKey();

    if (!start.isEmpty()) {
      start = CodecUtils.decode(start);
    }

    if (!end.isEmpty()) {
      end = CodecUtils.decode(end);
    }

    return builder.setStartKey(start).setEndKey(end).build();
  }
}
