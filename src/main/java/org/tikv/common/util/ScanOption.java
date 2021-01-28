/*
 * Copyright 2020 PingCAP, Inc.
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

package org.tikv.common.util;

import com.google.protobuf.ByteString;

/** if Limit of a ScanBatch is 0, it means to scan all keys in */
public class ScanOption {
  private final ByteString startKey;
  private final ByteString endKey;
  private final int limit;
  private final boolean keyOnly;

  private ScanOption(ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.limit = limit;
    this.keyOnly = keyOnly;
  }

  public ScanBatchBuilder newBuilder() {
    return new ScanBatchBuilder();
  }

  public ByteString getStartKey() {
    return startKey;
  }

  public ByteString getEndKey() {
    return endKey;
  }

  public int getLimit() {
    return limit;
  }

  public boolean isKeyOnly() {
    return keyOnly;
  }

  public static class ScanBatchBuilder {
    private ByteString startKey;
    private ByteString endKey;
    private int limit;
    private boolean keyOnly;

    private ScanBatchBuilder() {
      this.startKey = ByteString.EMPTY;
      this.endKey = ByteString.EMPTY;
      this.limit = 0;
      this.keyOnly = false;
    }

    public ScanOption build() {
      return new ScanOption(startKey, endKey, limit, keyOnly);
    }

    public ScanBatchBuilder setStartKey(ByteString startKey) {
      this.startKey = startKey;
      return this;
    }

    public ScanBatchBuilder setEndKey(ByteString endKey) {
      this.endKey = endKey;
      return this;
    }

    public ScanBatchBuilder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public ScanBatchBuilder setKeyOnly(boolean keyOnly) {
      this.keyOnly = keyOnly;
      return this;
    }
  }
}
