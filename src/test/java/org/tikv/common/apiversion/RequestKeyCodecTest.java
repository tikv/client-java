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

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.stream.Collectors;
import org.junit.Test;
import org.tikv.common.util.Pair;
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
    assertEquals(start, range.first);
    assertEquals(end, range.second);

    range = v1.encodePdQueryRange(start, end);
    assertEquals(start, range.first);
    assertEquals(end, range.second);

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
    assertEquals(CodecUtils.encode(start), range.first);
    assertEquals(CodecUtils.encode(end), range.second);

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
    assertEquals(ByteString.EMPTY, range.first);
    assertEquals(CodecUtils.encode(end), range.second);

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
    assertEquals(start, range.first);
    assertEquals(ByteString.EMPTY, range.second);
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
    assertEquals(CodecUtils.encode(v2.encodeKey(start)), range.first);
    assertEquals(CodecUtils.encode(v2.encodeKey(end)), range.second);

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
    assertEquals(CodecUtils.encode(v2.encodeKey(start)), range.first);
    assertEquals(CodecUtils.encode(v2.encodeKey(end)), range.second);

    // Test end is empty.
    end = ByteString.EMPTY;
    range = v2.encodeRange(start, end);
    assertEquals(v2.encodeKey(start), range.first);

    byte[] max = v2.encodeKey(ByteString.EMPTY).toByteArray();
    max[max.length - 1] += 1;
    assertArrayEquals(max, range.second.toByteArray());

    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(range.first))
            .setEndKey(CodecUtils.encode(range.second))
            .build();
    decoded = v2.decodeRegion(region);
    assertEquals(start, decoded.getStartKey());
    assertEquals(ByteString.EMPTY, decoded.getEndKey());

    // test region out of keyspace
    {
      ByteString m_123 = CodecUtils.encode(ByteString.copyFromUtf8("m_123"));
      ByteString m_124 = CodecUtils.encode(ByteString.copyFromUtf8("m_124"));
      ByteString infiniteEndKey_0 = CodecUtils.encode(v2.infiniteEndKey.concat(ByteString.copyFrom(new byte[] {0})));
      ByteString t_123 = CodecUtils.encode(ByteString.copyFromUtf8("t_123"));
      ByteString y_123 = CodecUtils.encode(ByteString.copyFromUtf8("y_123"));

      ByteString[][] outOfKeyspaceCases = {
          {ByteString.EMPTY, CodecUtils.encode(v2.keyPrefix)},    // ["", "r000"/"x000")
          {ByteString.EMPTY, m_123},
          {m_123, m_124},
          {m_124, CodecUtils.encode(v2.keyPrefix)},
          {CodecUtils.encode(v2.infiniteEndKey), ByteString.EMPTY}, // ["r001"/"x001", "")
          {CodecUtils.encode(v2.infiniteEndKey), infiniteEndKey_0},
          {infiniteEndKey_0, t_123},
          {y_123, ByteString.EMPTY}, // "y_123" is bigger than "infiniteEndKey" for both raw & txn.
      };
      
      for (ByteString[] testCase : outOfKeyspaceCases) {
        region =
            Region.newBuilder()
                .setStartKey(testCase[0])
                .setEndKey(testCase[1])
                .build();
        try {
          decoded = v2.decodeRegion(region);
          fail(String.format("[%s,%s): %s", testCase[0], testCase[1], decoded.toString()));
        } catch (Exception ignored) {
        }
      }
    }

    // case: regionStartKey == "" < keyPrefix < regionEndKey < infiniteEndKey
    region =
        Region.newBuilder()
            .setStartKey(ByteString.EMPTY)
            .setEndKey(CodecUtils.encode(v2.keyPrefix.concat(ByteString.copyFromUtf8("0"))))
            .build();
    decoded = v2.decodeRegion(region);
    assertTrue(decoded.getStartKey().isEmpty());
    assertEquals(ByteString.copyFromUtf8("0"), decoded.getEndKey());

    // case: "" < regionStartKey < keyPrefix < regionEndKey < infiniteEndKey < ""
    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(ByteString.copyFromUtf8("m_123")))
            .setEndKey(CodecUtils.encode(v2.keyPrefix.concat(ByteString.copyFromUtf8("0"))))
            .build();
    decoded = v2.decodeRegion(region);
    assertEquals(ByteString.EMPTY, decoded.getStartKey());
    assertEquals(ByteString.copyFromUtf8("0"), decoded.getEndKey());

    // case: "" < regionStartKey < keyPrefix < infiniteEndKey < regionEndKey < ""
    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(ByteString.copyFromUtf8("m_123")))
            .setEndKey(CodecUtils.encode(v2.infiniteEndKey.concat(ByteString.copyFromUtf8("0"))))
            .build();
    decoded = v2.decodeRegion(region);
    assertEquals(ByteString.EMPTY, decoded.getStartKey());
    assertEquals(ByteString.EMPTY, decoded.getEndKey());

    // case: keyPrefix < regionStartKey <  infiniteEndKey < regionEndKey < ""
    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(v2.keyPrefix.concat(ByteString.copyFromUtf8("0"))))
            .setEndKey(CodecUtils.encode(v2.infiniteEndKey.concat(ByteString.copyFromUtf8("0"))))
            .build();
    decoded = v2.decodeRegion(region);
    assertEquals(ByteString.copyFromUtf8("0"), decoded.getStartKey());
    assertTrue(decoded.getEndKey().isEmpty());

    // case: keyPrefix < regionStartKey <  infiniteEndKey < regionEndKey == ""
    region =
        Region.newBuilder()
            .setStartKey(CodecUtils.encode(v2.keyPrefix.concat(ByteString.copyFromUtf8("0"))))
            .setEndKey(ByteString.EMPTY)
            .build();
    decoded = v2.decodeRegion(region);
    assertEquals(ByteString.copyFromUtf8("0"), decoded.getStartKey());
    assertTrue(decoded.getEndKey().isEmpty());
  }
}
