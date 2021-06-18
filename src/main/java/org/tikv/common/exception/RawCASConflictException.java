/*
 * Copyright 2021 PingCAP, Inc.
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
 */

package org.tikv.common.exception;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.tikv.common.codec.KeyUtils;

public class RawCASConflictException extends RuntimeException {

  private final ByteString key;
  private final Optional<ByteString> expectedPrevValue;
  private final Optional<ByteString> prevValue;

  public RawCASConflictException(
      ByteString key, Optional<ByteString> expectedPrevValue, Optional<ByteString> prevValue) {
    super(
        String.format(
            "key=%s expectedPrevValue=%s prevValue=%s",
            KeyUtils.formatBytes(key), expectedPrevValue, prevValue));
    this.key = key;
    this.expectedPrevValue = expectedPrevValue;
    this.prevValue = prevValue;
  }

  public ByteString getKey() {
    return this.key;
  }

  public Optional<ByteString> getExpectedPrevValue() {
    return this.expectedPrevValue;
  }

  public Optional<ByteString> getPrevValue() {
    return this.prevValue;
  }
}
