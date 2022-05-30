package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.tikv.kvproto.Metapb;

public interface RequestKeyCodec {
  ByteString encodeKey(ByteString key);

  ByteString decodeKey(ByteString key);

  Pair<ByteString, ByteString> encodeRange(ByteString start, ByteString end);


  ByteString encodePdQuery(ByteString key);

  default Pair<ByteString, ByteString> encodePdQueryRange(ByteString start, ByteString end) {
    Pair<ByteString, ByteString> range = encodeRange(start, end);
    return Pair.of(CodecUtils.encode(range.getLeft()), CodecUtils.encode(range.getRight()));
  }

  Metapb.Region decodeRegion(Metapb.Region region);
}
