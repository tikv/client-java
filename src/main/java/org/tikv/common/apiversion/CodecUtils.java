package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;

public class CodecUtils {
  public static ByteString encode(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesCodec.writeBytes(cdo, key.toByteArray());
    return cdo.toByteString();
  }

  public static ByteString decode(ByteString key) {
    return ByteString.copyFrom(BytesCodec.readBytes(new CodecDataInput(key)));
  }
}
