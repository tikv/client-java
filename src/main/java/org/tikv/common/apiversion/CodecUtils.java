/*
 * Copyright 2022 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.apiversion;

import com.google.protobuf.ByteString;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;

// TODO(iosmanthus): use ByteString.wrap to avoid once more copying.
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
