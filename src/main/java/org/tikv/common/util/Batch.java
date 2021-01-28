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
import java.util.List;
import org.tikv.common.region.TiRegion;

/** A Batch containing the region, a list of keys and/or values to send */
public class Batch {
  public final TiRegion region;
  public final List<ByteString> keys;
  public final List<ByteString> values;

  public Batch(TiRegion region, List<ByteString> keys, List<ByteString> values) {
    this.region = region;
    this.keys = keys;
    this.values = values;
  }
}
