/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.br;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Brpb;

public class BackupDecoderTest {

  private static final int TOTAL_COUNT = 500;
  private static final String KEY_PREFIX = "test_";
  private static final String VALUE =
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

  @Test
  public void rawKVSSTDecoderTest() throws RocksDBException, IOException {
    String backupmetaFilePath = "src/test/resources/sst/backupmeta";
    String sst1FilePath =
        "src/test/resources/sst/1_2_2_7154800cc311f03afd1532e961b9a878dfbb119b104cf4daad5d0c7c0eacb502_1633919546277_default.sst";
    String sst2FilePath =
        "src/test/resources/sst/4_8_2_9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08_1633919546278_default.sst";

    BackupMetaDecoder backupMetaDecoder = BackupMetaDecoder.parse(backupmetaFilePath);
    Brpb.BackupMeta backupMeta = backupMetaDecoder.getBackupMeta();

    BackupDecoder sstBackup = new BackupDecoder(backupMeta);

    decodeSST(sstBackup, sst1FilePath);
    decodeSST(sstBackup, sst2FilePath);
  }

  @Test
  public void rawKVWithTTLSSTDecoderTest() throws RocksDBException, IOException {
    String backupmetaFilePath = "src/test/resources/sst_ttl/backupmeta";
    String sst1FilePath =
        "src/test/resources/sst_ttl/1_2_2_7154800cc311f03afd1532e961b9a878dfbb119b104cf4daad5d0c7c0eacb502_1634199092593_default.sst";
    String sst2FilePath =
        "src/test/resources/sst_ttl/5_8_2_9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08_1634199092587_default.sst";

    BackupMetaDecoder backupMetaDecoder = BackupMetaDecoder.parse(backupmetaFilePath);
    Brpb.BackupMeta backupMeta = backupMetaDecoder.getBackupMeta();

    BackupDecoder sstBackup = new BackupDecoder(backupMeta, true);

    decodeSST(sstBackup, sst1FilePath);
    decodeSST(sstBackup, sst2FilePath);
  }

  private void decodeSST(BackupDecoder sstBackup, String sst) throws RocksDBException {
    String fileName = new File(sst).getName();
    Brpb.File backupFile =
        sstBackup
            .getBackupMeta()
            .getFilesList()
            .stream()
            .filter(a -> a.getName().equals(fileName))
            .findFirst()
            .get();
    Assert.assertEquals(TOTAL_COUNT, backupFile.getTotalKvs());

    SSTDecoder sstDecoder = sstBackup.decodeSST(sst);
    Iterator<Pair<ByteString, ByteString>> iterator = sstDecoder.getIterator();
    int count = 0;
    while (iterator.hasNext()) {
      Pair<ByteString, ByteString> pair = iterator.next();
      Assert.assertEquals(VALUE, pair.second.toStringUtf8());
      Assert.assertTrue(pair.first.toStringUtf8().startsWith(KEY_PREFIX));
      count += 1;
    }
    sstDecoder.close();
    Assert.assertEquals(TOTAL_COUNT, count);
  }
}
