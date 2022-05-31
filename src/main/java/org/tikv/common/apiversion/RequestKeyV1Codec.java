package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.tikv.kvproto.Kvrpcpb.Mutation;
import org.tikv.kvproto.Metapb.Region;

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
  public Pair<ByteString, ByteString> encodeRange(ByteString start, ByteString end) {
    return Pair.of(start, end);
  }

  @Override
  public ByteString encodePdQuery(ByteString key) {
    return key;
  }

  @Override
  public Pair<ByteString, ByteString> encodePdQueryRange(ByteString start, ByteString end) {
    return Pair.of(start, end);
  }

  @Override
  public Region decodeRegion(Region region) {
    return region;
  }
}
