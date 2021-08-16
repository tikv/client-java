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
import org.tikv.common.codec.Codec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Metapb;

public class TxnImporterClient {
  private TiConfiguration tiConf;
  private TiSession tiSession;
  private ByteString uuid;
  private Key minKey;
  private Key maxKey;
  private TiRegion region;

  private boolean streamOpened = false;
  private ImportSstpb.SSTMeta sstMeta;
  private List<TxnImporterStoreClient> clientList;
  private TxnImporterStoreClient clientLeader;

  public TxnImporterClient(
      TiSession tiSession, ByteString uuid, Key minKey, Key maxKey, TiRegion region) {
    this.uuid = uuid;
    this.tiConf = tiSession.getConf();
    this.tiSession = tiSession;
    this.minKey = minKey;
    this.maxKey = maxKey;
    this.region = region;
  }

  public void txnWrite(Iterator<Pair<ByteString, ByteString>> iterator) throws GrpcException {

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
        startTxnWrite();
        txnWriteMeta();
        streamOpened = true;
      }
      txnWriteBatch(pairs);
    }

    if (streamOpened) {
      finishTxnWrite();
      ingest();
    }
  }

  // TODO
  private ByteString encode(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    Codec.BytesCodec.writeBytes(cdo, key.toByteArray());
    ByteString key2 = cdo.toByteString();
    return key2;
  }

  private void init() {
    long regionId = region.getId();
    Metapb.RegionEpoch regionEpoch = region.getRegionEpoch();
    ImportSstpb.Range range =
        ImportSstpb.Range.newBuilder()
            .setStart(encode(minKey.toByteString()))
            .setEnd(encode(maxKey.toByteString()))
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
      TxnImporterStoreClient importerStoreClient =
          tiSession.getTxnImporterRegionStoreClientBuilder().build(store);
      clientList.add(importerStoreClient);

      if (region.getLeader().getStoreId() == storeId) {
        clientLeader = importerStoreClient;
      }
    }
  }

  private void startTxnWrite() {
    for (TxnImporterStoreClient client : clientList) {
      client.startTxnWrite();
    }
  }

  private void txnWriteMeta() {
    ImportSstpb.WriteRequest request =
        ImportSstpb.WriteRequest.newBuilder().setMeta(sstMeta).build();
    for (TxnImporterStoreClient client : clientList) {
      client.txnWriteBatch(request);
    }
  }

  private void txnWriteBatch(List<ImportSstpb.Pair> pairs) {
    ImportSstpb.WriteBatch batch = ImportSstpb.WriteBatch.newBuilder().addAllPairs(pairs).setCommitTs(10).build();

    ImportSstpb.WriteRequest request =
        ImportSstpb.WriteRequest.newBuilder().setMeta(sstMeta).setBatch(batch).build();
    for (TxnImporterStoreClient client : clientList) {
      client.txnWriteBatch(request);
    }
  }

  private void finishTxnWrite() {
    for (TxnImporterStoreClient client : clientList) {
      client.finishTxnWrite();
    }
  }

  private void ingest() throws GrpcException {
    List<TxnImporterStoreClient> workingClients = new ArrayList<>(clientList);
    while (!workingClients.isEmpty()) {
      Iterator<TxnImporterStoreClient> itor = workingClients.iterator();
      while (itor.hasNext()) {
        TxnImporterStoreClient client = itor.next();
        if (client.isTxnWriteResponseReceived()) {
          itor.remove();
        } else if (client.hasTxnWriteResponseError()) {
          throw new GrpcException(client.getTxnWriteError());
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
