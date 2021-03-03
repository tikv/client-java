package org.tikv.common.util;

import com.google.protobuf.ByteString;
import org.tikv.common.region.TiRegion;

public class DeleteRange {
  public final TiRegion region;
  public final ByteString startKey;
  public final ByteString endKey;

  public DeleteRange(TiRegion region, ByteString startKey, ByteString endKey) {
    this.region = region;
    this.startKey = startKey;
    this.endKey = endKey;
  }
}
