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

package org.tikv.common.util;

import com.google.protobuf.ByteString;
import org.tikv.common.region.TiRegion;

public class DeleteRange {
  private final BackOffer backOffer;
  private final TiRegion region;
  private final ByteString startKey;
  private final ByteString endKey;

  public DeleteRange(BackOffer backOffer, TiRegion region, ByteString startKey, ByteString endKey) {
    this.backOffer = ConcreteBackOffer.create(backOffer);
    this.region = region;
    this.startKey = startKey;
    this.endKey = endKey;
  }

  public BackOffer getBackOffer() {
    return backOffer;
  }

  public TiRegion getRegion() {
    return region;
  }

  public ByteString getStartKey() {
    return startKey;
  }

  public ByteString getEndKey() {
    return endKey;
  }

  @Override
  public String toString() {
    return KeyRangeUtils.makeRange(getStartKey(), getEndKey()).toString();
  }
}
