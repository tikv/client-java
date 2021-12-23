/*
 * Copyright 2017 TiKV Project Authors.
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

package org.tikv.common.predicates;

import com.google.common.collect.Range;
import org.tikv.common.key.Key;
import org.tikv.common.key.TypedKey;

public class IndexRange {
  private final Key accessKey;
  private final Range<TypedKey> range;

  public IndexRange(Key accessKey, Range<TypedKey> range) {
    this.accessKey = accessKey;
    this.range = range;
  }

  public Key getAccessKey() {
    return accessKey;
  }

  public boolean hasAccessKey() {
    return accessKey != null;
  }

  public boolean hasRange() {
    return range != null;
  }

  public Range<TypedKey> getRange() {
    return range;
  }

  @Override
  public String toString() {
    return "accessKey: " + accessKey + " range: " + range;
  }
}
