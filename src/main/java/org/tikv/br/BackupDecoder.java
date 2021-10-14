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

import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.tikv.common.exception.SSTDecodeException;
import org.tikv.kvproto.Brpb;

public class BackupDecoder {
  private final Brpb.BackupMeta backupMeta;
  private final boolean ttlEnabled;
  private final KVDecoder kvDecoder;

  public BackupDecoder(Brpb.BackupMeta backupMeta) throws SSTDecodeException {
    this.backupMeta = backupMeta;
    this.ttlEnabled = false;
    this.kvDecoder = initKVDecoder();
  }

  public BackupDecoder(Brpb.BackupMeta backupMeta, boolean ttlEnabled) throws SSTDecodeException {
    this.backupMeta = backupMeta;
    this.ttlEnabled = ttlEnabled;
    this.kvDecoder = initKVDecoder();
  }

  private KVDecoder initKVDecoder() throws SSTDecodeException {
    // Currently only v1 is supported.
    // V2 will be added after https://github.com/tikv/tikv/issues/10938.
    if (backupMeta.getIsRawKv()) {
      // TODO: ttl_enable should be witten to BackupMeta
      return new RawKVDecoderV1(ttlEnabled);
    } else {
      throw new SSTDecodeException("TxnKV is not supported yet!");
    }
  }

  public SSTDecoder decodeSST(String sstFilePath) {
    return decodeSST(sstFilePath, new Options(), new ReadOptions());
  }

  public SSTDecoder decodeSST(String sstFilePath, Options options, ReadOptions readOptions) {
    return new SSTDecoder(sstFilePath, kvDecoder, options, readOptions);
  }

  public Brpb.BackupMeta getBackupMeta() {
    return backupMeta;
  }
}
