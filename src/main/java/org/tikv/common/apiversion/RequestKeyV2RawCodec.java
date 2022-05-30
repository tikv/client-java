package org.tikv.common.apiversion;

public class RequestKeyV2RawCodec extends RequestKeyV2Codec {
  public RequestKeyV2RawCodec() {
    super();

    this.keyPrefix = RAW_KEY_PREFIX;
    this.infiniteEndKey = RAW_END_KEY;
  }
}
