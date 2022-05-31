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

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.tikv.kvproto.Metapb.Region;

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
  }
}
