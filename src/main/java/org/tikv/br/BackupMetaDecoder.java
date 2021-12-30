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

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.tikv.kvproto.Brpb;

public class BackupMetaDecoder {
  private final Brpb.BackupMeta backupMeta;

  public BackupMetaDecoder(byte[] data) throws InvalidProtocolBufferException {
    this.backupMeta = Brpb.BackupMeta.parseFrom(data);
  }

  public Brpb.BackupMeta getBackupMeta() {
    return backupMeta;
  }

  public static BackupMetaDecoder parse(String backupMetaFilePath) throws IOException {
    byte[] data = Files.readAllBytes(new File(backupMetaFilePath).toPath());
    return new BackupMetaDecoder(data);
  }
}
