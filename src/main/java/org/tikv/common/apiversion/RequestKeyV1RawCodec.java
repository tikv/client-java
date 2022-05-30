package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.tikv.kvproto.Metapb;

public class RequestKeyV1RawCodec implements RequestKeyCodec {
  public RequestKeyV1RawCodec() {}

  @Override
  public ByteString encodeKey(ByteString key) {
    return key;
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
  public Metapb.Region decodeRegion(Metapb.Region region) {
    return region;
  }
}
