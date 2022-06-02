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
import java.util.List;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Kvrpcpb.Mutation;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.Pdpb;

public class RequestKeyV1Codec implements RequestKeyCodec {
  @Override
  public ByteString encodeKey(ByteString key) {
    return key;
  }

  @Override
  public List<ByteString> encodeKeys(List<ByteString> keys) {
    return keys;
  }

  @Override
  public List<Mutation> encodeMutations(List<Mutation> mutations) {
    return mutations;
  }

  @Override
  public ByteString decodeKey(ByteString key) {
    return key;
  }

  @Override
  public KvPair decodeKvPair(KvPair pair) {
    return pair;
  }

  @Override
  public List<KvPair> decodeKvPairs(List<KvPair> pairs) {
    return pairs;
  }

  @Override
  public Pair<ByteString, ByteString> encodeRange(ByteString start, ByteString end) {
    return Pair.create(start, end);
  }

  @Override
  public ByteString encodePdQuery(ByteString key) {
    return key;
  }

  @Override
  public Pair<ByteString, ByteString> encodePdQueryRange(ByteString start, ByteString end) {
    return Pair.create(start, end);
  }

  @Override
  public Region decodeRegion(Region region) {
    return region;
  }

  @Override
  public List<Pdpb.Region> decodePdRegions(List<Pdpb.Region> regions) {
    return regions;
  }
}
