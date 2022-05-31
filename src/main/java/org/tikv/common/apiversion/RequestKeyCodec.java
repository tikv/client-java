package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.tikv.kvproto.Kvrpcpb.Mutation;
import org.tikv.kvproto.Metapb;

public interface RequestKeyCodec {
  ByteString encodeKey(ByteString key);

  default List<ByteString> encodeKeys(List<ByteString> keys) {
    return keys.stream().map(this::encodeKey).collect(Collectors.toList());
  }

  default List<Mutation> encodeMutations(List<Mutation> mutations) {
    return mutations
        .stream()
        .map(mut -> Mutation.newBuilder().mergeFrom(mut).setKey(encodeKey(mut.getKey())).build())
        .collect(Collectors.toList());
  }

  ByteString decodeKey(ByteString key);

  Pair<ByteString, ByteString> encodeRange(ByteString start, ByteString end);

  ByteString encodePdQuery(ByteString key);

  Pair<ByteString, ByteString> encodePdQueryRange(ByteString start, ByteString end);

  Metapb.Region decodeRegion(Metapb.Region region);
}
