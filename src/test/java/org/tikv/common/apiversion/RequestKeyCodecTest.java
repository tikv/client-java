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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.Pdpb;

public class RequestKeyCodecTest {
  @Test
  public void testV1RawCodec() {
    RequestKeyCodec v1 = new RequestKeyV1RawCodec();
    ByteString key = ByteString.copyFromUtf8("testV1RawCodec");

    assertEquals(key, v1.encodeKey(key));
    assertEquals(key, v1.decodeKey(v1.encodeKey(key)));

    assertEquals(key, v1.encodePdQuery(key));

    ByteString start = ByteString.copyFromUtf8("testV1RawCodec_start");
    ByteString end = ByteString.copyFromUtf8("testV1RawCodec_end");
    Pair<ByteString, ByteString> range = v1.encodeRange(start, end);
    assertEquals(start, range.getLeft());
    assertEquals(end, range.getRight());

    range = v1.encodePdQueryRange(start, end);
    assertEquals(start, range.getLeft());
    assertEquals(end, range.getRight());

    Region region = Region.newBuilder().setStartKey(start).setEndKey(end).build();
    assertEquals(region, v1.decodeRegion(region));

    assertEquals(
        ImmutableList.of(region),
        v1.decodePdRegions(ImmutableList.of(Pdpb.Region.newBuilder().setRegion(region).build()))
            .stream()
            .map(Pdpb.Region::getRegion)
            .collect(Collectors.toList()));
  }

  @Test
  public void testV1TxnCodec() {
    RequestKeyCodec v1 = new RequestKeyV1TxnCodec();

    ByteString key = ByteString.copyFromUtf8("testV1TxnCodec");

    assertEquals(CodecUtils.encode(key), v1.encodePdQuery(key));

    ByteString start = ByteString.copyFromUtf8("testV1TxnCodec_start");
    ByteString end = ByteString.copyFromUtf8("testV1TxnCodec_end");

    // Test start and end are both non-empty.
    Pair<ByteString, ByteString> range = v1.encodePdQueryRange(start, end);
    assertEquals(CodecUtils.encode(start), range.getLeft());
    assertEquals(CodecUtils.encode(end), range.getRight());

    Region region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(start))
            .setEndKey(CodecUtils.encode(end))
            .build();
    Region decoded = v1.decodeRegion(region);
    assertEquals(start, decoded.getStartKey());
    assertEquals(end, decoded.getEndKey());

    // Test start is empty.
    start = ByteString.EMPTY;
    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(start))
            .setEndKey(CodecUtils.encode(end))
            .build();
    decoded = v1.decodeRegion(region);
    assertEquals(start, decoded.getStartKey());
    assertEquals(end, decoded.getEndKey());

    range = v1.encodePdQueryRange(start, end);
    assertEquals(ByteString.EMPTY, range.getLeft());
    assertEquals(CodecUtils.encode(end), range.getRight());

    // Test end is empty.
    end = ByteString.EMPTY;
    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(start))
            .setEndKey(CodecUtils.encode(end))
            .build();
    decoded = v1.decodeRegion(region);
    assertEquals(start, decoded.getStartKey());
    assertEquals(ByteString.EMPTY, decoded.getEndKey());

    range = v1.encodePdQueryRange(start, end);
    assertEquals(start, range.getLeft());
    assertEquals(ByteString.EMPTY, range.getRight());
  }

  @Test
  public void testV2Codec() {
    testV2Codec(new RequestKeyV2RawCodec());
    testV2Codec(new RequestKeyV2TxnCodec());
  }

  void testV2Codec(RequestKeyV2Codec v2) {
    ByteString key = ByteString.copyFromUtf8("testV2RawCodec");

    assertEquals(key, v2.decodeKey(v2.encodeKey(key)));
    assertEquals(CodecUtils.encode(v2.encodeKey(key)), v2.encodePdQuery(key));

    ByteString start = ByteString.copyFromUtf8("testV1TxnCodec_start");
    ByteString end = ByteString.copyFromUtf8("testV1TxnCodec_end");

    // Test start and end are both non-empty.
    Pair<ByteString, ByteString> range = v2.encodePdQueryRange(start, end);
    assertEquals(CodecUtils.encode(v2.encodeKey(start)), range.getLeft());
    assertEquals(CodecUtils.encode(v2.encodeKey(end)), range.getRight());

    Region region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(v2.encodeKey(start)))
            .setEndKey(CodecUtils.encode(v2.encodeKey(end)))
            .build();
    Region decoded = v2.decodeRegion(region);
    assertEquals(start, decoded.getStartKey());
    assertEquals(end, decoded.getEndKey());

    // Test start is empty.
    start = ByteString.EMPTY;
    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(v2.encodeKey(start)))
            .setEndKey(CodecUtils.encode(v2.encodeKey(end)))
            .build();
    decoded = v2.decodeRegion(region);
    assertEquals(start, decoded.getStartKey());
    assertEquals(end, decoded.getEndKey());

    range = v2.encodePdQueryRange(start, end);
    assertEquals(CodecUtils.encode(v2.encodeKey(start)), range.getLeft());
    assertEquals(CodecUtils.encode(v2.encodeKey(end)), range.getRight());

    // Test end is empty.
    end = ByteString.EMPTY;
    range = v2.encodeRange(start, end);
    assertEquals(v2.encodeKey(start), range.getLeft());
    assertArrayEquals(
        new byte[] {(byte) (v2.encodeKey(ByteString.EMPTY).byteAt(0) + 1)},
        range.getRight().toByteArray());

    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(range.getLeft()))
            .setEndKey(CodecUtils.encode(range.getRight()))
            .build();
    decoded = v2.decodeRegion(region);
    assertEquals(start, decoded.getStartKey());
    assertEquals(ByteString.EMPTY, decoded.getEndKey());
  }
}
