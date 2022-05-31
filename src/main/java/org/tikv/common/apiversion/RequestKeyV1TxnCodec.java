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
    return Pair.of(CodecUtils.encode(start), CodecUtils.encode(end));
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
