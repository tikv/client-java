package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Region;

public class RequestKeyV2Codec implements RequestKeyCodec {
  protected static ByteString RAW_KEY_PREFIX = ByteString.copyFromUtf8("r");
  protected static ByteString RAW_END_KEY =
      ByteString.copyFrom(new byte[]{(byte) (RAW_KEY_PREFIX.toByteArray()[0] + 1)});

  protected static ByteString TXN_KEY_PREFIX = ByteString.copyFromUtf8("x");
  protected static ByteString TXN_END_KEY =
      ByteString.copyFrom(new byte[]{(byte) (TXN_KEY_PREFIX.toByteArray()[0] + 1)});
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

    return key.substring(1);
  }

  @Override
  public Pair<ByteString, ByteString> encodeRange(ByteString start, ByteString end) {
    start = encodeKey(start);

    end = end.isEmpty() ? infiniteEndKey : encodeKey(end);

    return Pair.of(start, end);
  }

  @Override
  public ByteString encodePdQuery(ByteString key) {
    return CodecUtils.encode(encodeKey(key));
  }

  @Override
  public Region decodeRegion(Region region) {
    Metapb.Region.Builder builder = Metapb.Region.newBuilder().mergeFrom(region);

    ByteString start = region.getStartKey();
    ByteString end = region.getEndKey();

    if (!start.isEmpty()) {
      start = CodecUtils.decode(start);
      if (ByteString.unsignedLexicographicalComparator().compare(start, keyPrefix) < 0) {
        start = ByteString.EMPTY;
      } else {
        start = decodeKey(start);
      }
    }

    if (!end.isEmpty()) {
      end = CodecUtils.decode(end);
      if (ByteString.unsignedLexicographicalComparator().compare(end, infiniteEndKey) >= 0) {
        end = ByteString.EMPTY;
      } else {
        end = decodeKey(end);
      }
    }

    return builder.setStartKey(start).setEndKey(end).build();
  }
}
