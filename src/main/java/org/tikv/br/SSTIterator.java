/*
 *
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
 *
 */

package org.tikv.br;

import com.google.protobuf.ByteString;
import java.util.Iterator;
import org.rocksdb.SstFileReaderIterator;
import org.tikv.common.util.Pair;

public class SSTIterator implements Iterator<Pair<ByteString, ByteString>> {
  private final SstFileReaderIterator iterator;
  private final KVDecoder kvDecoder;

  private Pair<ByteString, ByteString> nextPair;

  public SSTIterator(SstFileReaderIterator iterator, KVDecoder kvDecoder) {
    this.iterator = iterator;
    this.kvDecoder = kvDecoder;
    this.iterator.seekToFirst();
    this.nextPair = processNext();
  }

  @Override
  public boolean hasNext() {
    return nextPair != null;
  }

  @Override
  public Pair<ByteString, ByteString> next() {
    Pair<ByteString, ByteString> result = nextPair;
    nextPair = processNext();
    return result;
  }

  private Pair<ByteString, ByteString> processNext() {
    if (iterator.isValid()) {
      ByteString key = kvDecoder.decodeKey(iterator.key());
      ByteString value = kvDecoder.decodeValue(iterator.value());
      iterator.next();
      if (key != null) {
        return Pair.create(key, value);
      } else {
        return processNext();
      }
    } else {
      return null;
    }
  }
}
