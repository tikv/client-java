/*
 *
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.br;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawKVDecoderV1 implements KVDecoder {
  private static final Logger logger = LoggerFactory.getLogger(SSTIterator.class);

  private final boolean ttlEnabled;

  public RawKVDecoderV1(boolean ttlEnabled) {
    this.ttlEnabled = ttlEnabled;
  }

  @Override
  public ByteString decodeKey(byte[] key) {
    if (key == null || key.length == 0) {
      logger.warn(
          "skip Key-Value pair because key == null || key.length == 0, key = "
              + Arrays.toString(key));
      return null;
    } else if (key[0] != 'z') {
      logger.warn("skip Key-Value pair because key[0] != 'z', key = " + Arrays.toString(key));
      return null;
    }
    return ByteString.copyFrom(key, 1, key.length - 1);
  }

  @Override
  public ByteString decodeValue(byte[] value) {
    if (!ttlEnabled) {
      return ByteString.copyFrom(value);
    } else {
      return ByteString.copyFrom(value).substring(0, value.length - 8);
    }
  }
}
