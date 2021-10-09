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

package org.tikv.sst;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Iterator;
import org.rocksdb.SstFileReaderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.util.Pair;

public class SSTIterator implements Iterator<Pair<ByteString, ByteString>> {
  private static final Logger logger = LoggerFactory.getLogger(SSTIterator.class);

  private final SstFileReaderIterator iterator;

  private Pair<ByteString, ByteString> nextPair;

  public SSTIterator(SstFileReaderIterator iterator) {
    this.iterator = iterator;
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
      ByteString key = decodeKey(iterator.key());
      ByteString value = decodeValue(iterator.value());
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

  private ByteString decodeKey(byte[] key) {
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

  private ByteString decodeValue(byte[] value) {
    return ByteString.copyFrom(value);
  }
}
