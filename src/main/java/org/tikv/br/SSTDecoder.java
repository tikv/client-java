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
import java.util.Iterator;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.tikv.common.util.Pair;

public class SSTDecoder {
  private final String filePath;
  private final KVDecoder kvDecoder;
  private final Options options;
  private final ReadOptions readOptions;

  private SstFileReader sstFileReader;
  private SstFileReaderIterator iterator;

  public SSTDecoder(String sstFilePath, KVDecoder kvDecoder) {
    this.filePath = sstFilePath;
    this.kvDecoder = kvDecoder;
    this.options = new Options();
    this.readOptions = new ReadOptions();
  }

  public SSTDecoder(
      String filePath, KVDecoder kvDecoder, Options options, ReadOptions readOptions) {
    this.filePath = filePath;
    this.kvDecoder = kvDecoder;
    this.options = options;
    this.readOptions = readOptions;
  }

  public synchronized Iterator<Pair<ByteString, ByteString>> getIterator() throws RocksDBException {
    if (sstFileReader != null || iterator != null) {
      throw new RocksDBException("File already opened!");
    }

    sstFileReader = new SstFileReader(new Options());
    sstFileReader.open(filePath);
    iterator = sstFileReader.newIterator(new ReadOptions());
    return new SSTIterator(iterator, kvDecoder);
  }

  public synchronized void close() {
    try {
      if (iterator != null) {
        iterator.close();
      }
    } finally {
      iterator = null;
    }

    try {
      if (sstFileReader != null) {
        sstFileReader.close();
      }
    } finally {
      sstFileReader = null;
    }
  }

  public String getFilePath() {
    return filePath;
  }

  public Options getOptions() {
    return options;
  }

  public ReadOptions getReadOptions() {
    return readOptions;
  }
}
