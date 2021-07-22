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

package org.tikv.common.importer;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Metapb;

public class ImporterClient {
  private TiConfiguration tiConf;
  private TiSession tiSession;
  private ByteString uuid;
  private Key minKey;
  private Key maxKey;
  private TiRegion region;
  private Long ttl;

  private boolean streamOpened = false;
  private ImportSstpb.SSTMeta sstMeta;
  private List<ImporterStoreClient> clientList;
  private ImporterStoreClient clientLeader;

  public ImporterClient(
      TiSession tiSession, ByteString uuid, Key minKey, Key maxKey, TiRegion region, Long ttl) {
    this.uuid = uuid;
    this.tiConf = tiSession.getConf();
    this.tiSession = tiSession;
    this.minKey = minKey;
    this.maxKey = maxKey;
    this.region = region;
    this.ttl = ttl;
  }

  /**
   * write KV pairs to RawKV using KVStream interface
   *
   * @param iterator
   */
  public void rawWrite(Iterator<Pair<ByteString, ByteString>> iterator) throws GrpcException {
    if (!tiConf.isRawKVMode()) {
      throw new IllegalArgumentException("KVMode is not RAW in TiConfiguration!");
    }

    streamOpened = false;

    int maxKVBatchSize = tiConf.getImporterMaxKVBatchSize();
    int maxKVBatchBytes = tiConf.getImporterMaxKVBatchBytes();
    int totalBytes = 0;
    while (iterator.hasNext()) {
      ArrayList<ImportSstpb.Pair> pairs = new ArrayList<>(maxKVBatchSize);
      for (int i = 0; i < maxKVBatchSize; i++) {
        if (iterator.hasNext()) {
          Pair<ByteString, ByteString> pair = iterator.next();
          pairs.add(ImportSstpb.Pair.newBuilder().setKey(pair.first).setValue(pair.second).build());
          totalBytes += (pair.first.size() + pair.second.size());
        }
        if (totalBytes > maxKVBatchBytes) {
          break;
        }
      }
      if (!streamOpened) {
        init();
        startRawWrite();
        rawWriteMeta();
        streamOpened = true;
      }
      rawWriteBatch(pairs);
    }

    if (streamOpened) {
      finishRawWrite();
      ingest();
    }
  }

  private void init() {
    long regionId = region.getId();
    Metapb.RegionEpoch regionEpoch = region.getRegionEpoch();
    ImportSstpb.Range range =
        ImportSstpb.Range.newBuilder()
            .setStart(minKey.toByteString())
            .setEnd(maxKey.toByteString())
            .build();

    sstMeta =
        ImportSstpb.SSTMeta.newBuilder()
            .setUuid(uuid)
            .setRegionId(regionId)
            .setRegionEpoch(regionEpoch)
            .setRange(range)
            .build();

    clientList = new ArrayList<>();
    for (Metapb.Peer peer : region.getPeersList()) {
      long storeId = peer.getStoreId();
      TiStore store = tiSession.getRegionManager().getStoreById(storeId);
      ImporterStoreClient importerStoreClient =
          tiSession.getImporterRegionStoreClientBuilder().build(store);
      clientList.add(importerStoreClient);

      if (region.getLeader().getStoreId() == storeId) {
        clientLeader = importerStoreClient;
      }
    }
  }

  private void startRawWrite() {
    for (ImporterStoreClient client : clientList) {
      client.startRawWrite();
    }
  }

  private void rawWriteMeta() {
    ImportSstpb.RawWriteRequest request =
        ImportSstpb.RawWriteRequest.newBuilder().setMeta(sstMeta).build();
    for (ImporterStoreClient client : clientList) {
      client.rawWriteBatch(request);
    }
  }

  private void rawWriteBatch(List<ImportSstpb.Pair> pairs) {
    ImportSstpb.RawWriteBatch batch;

    if (ttl == null || ttl <= 0) {
      batch = ImportSstpb.RawWriteBatch.newBuilder().addAllPairs(pairs).build();
    } else {
      batch = ImportSstpb.RawWriteBatch.newBuilder().addAllPairs(pairs).setTtl(ttl).build();
    }

    ImportSstpb.RawWriteRequest request =
        ImportSstpb.RawWriteRequest.newBuilder().setBatch(batch).build();
    for (ImporterStoreClient client : clientList) {
      client.rawWriteBatch(request);
    }
  }

  private void finishRawWrite() {
    for (ImporterStoreClient client : clientList) {
      client.finishRawWrite();
    }
  }

  private void ingest() throws GrpcException {
    List<ImporterStoreClient> workingClients = new ArrayList<>(clientList);
    while (!workingClients.isEmpty()) {
      Iterator<ImporterStoreClient> itor = workingClients.iterator();
      while (itor.hasNext()) {
        ImporterStoreClient client = itor.next();
        if (client.isRawWriteResponseReceived()) {
          itor.remove();
        } else if (client.hasRawWriteResponseError()) {
          throw new GrpcException(client.getRawWriteError());
        }
      }

      if (!workingClients.isEmpty()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    clientLeader.multiIngest(region.getLeaderContext());
  }
}
