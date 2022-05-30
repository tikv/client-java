package org.tikv.common.apiversion;

public class RequestKeyV2TxnCodec extends RequestKeyV2Codec {
  public RequestKeyV2TxnCodec() {
    super();

    this.keyPrefix = TXN_KEY_PREFIX;
    this.infiniteEndKey = TXN_END_KEY;
  }
}
