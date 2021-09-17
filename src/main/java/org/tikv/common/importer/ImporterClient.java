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

import static org.tikv.common.operation.RegionErrorHandler.NO_LEADER_STORE_ID;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.Codec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.RegionException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Errorpb.Error;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Metapb;

public class ImporterClient {
  private static final Logger logger = LoggerFactory.getLogger(ImporterClient.class);

  private TiConfiguration tiConf;
  private TiSession tiSession;
  private ByteString uuid;
  private Key minKey;
  private Key maxKey;
  private TiRegion region;
  private Long ttl;

  private boolean deduplicate = false;

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

  public boolean isDeduplicate() {
    return deduplicate;
  }

  public void setDeduplicate(boolean deduplicate) {
    this.deduplicate = deduplicate;
  }

  /**
   * write KV pairs to RawKV/Txn using KVStream interface
   *
   * @param iterator
   */
  public void write(Iterator<Pair<ByteString, ByteString>> iterator) throws TiKVException {

    streamOpened = false;

    int maxKVBatchSize = tiConf.getImporterMaxKVBatchSize();
    int maxKVBatchBytes = tiConf.getImporterMaxKVBatchBytes();
    int totalBytes = 0;
    ByteString preKey = null;
    while (iterator.hasNext()) {
      ArrayList<ImportSstpb.Pair> pairs = new ArrayList<>(maxKVBatchSize);
      for (int i = 0; i < maxKVBatchSize; i++) {
        if (iterator.hasNext()) {
          Pair<ByteString, ByteString> pair = iterator.next();
          if (preKey != null && preKey.equals(pair.first)) {
            if (deduplicate) {
              logger.info("skip duplicate key: {}", preKey.toStringUtf8());
            } else {
              throw new TiKVException(
                  String.format("duplicate key found, key = %s", preKey.toStringUtf8()));
            }
          } else {
            pairs.add(
                ImportSstpb.Pair.newBuilder().setKey(pair.first).setValue(pair.second).build());
            totalBytes += (pair.first.size() + pair.second.size());
            preKey = pair.first;
          }
        }
        if (totalBytes > maxKVBatchBytes || !iterator.hasNext()) {
          break;
        }
      }
      if (!streamOpened) {
        init();
        startWrite();
        writeMeta();
        streamOpened = true;
      }
      writeBatch(pairs);
      totalBytes = 0;
    }

    if (streamOpened) {
      finishWrite();
      ingest();
    }
  }

  private void init() {
    long regionId = region.getId();
    Metapb.RegionEpoch regionEpoch = region.getRegionEpoch();
    ImportSstpb.Range range =
        tiConf.isTxnKVMode()
            ? ImportSstpb.Range.newBuilder()
                .setStart(encode(minKey.toByteString()))
                .setEnd(encode(maxKey.toByteString()))
                .build()
            : ImportSstpb.Range.newBuilder()
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

  private ByteString encode(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    Codec.BytesCodec.writeBytes(cdo, key.toByteArray());
    return cdo.toByteString();
  }

  private void startWrite() {
    for (ImporterStoreClient client : clientList) {
      client.startWrite();
    }
  }

  private void writeMeta() {
    if (tiConf.isTxnKVMode()) {
      ImportSstpb.WriteRequest request =
          ImportSstpb.WriteRequest.newBuilder().setMeta(sstMeta).build();
      for (ImporterStoreClient client : clientList) {
        client.writeBatch(request);
      }
    } else {
      ImportSstpb.RawWriteRequest request =
          ImportSstpb.RawWriteRequest.newBuilder().setMeta(sstMeta).build();
      for (ImporterStoreClient client : clientList) {
        client.writeBatch(request);
      }
    }
  }

  private void writeBatch(List<ImportSstpb.Pair> pairs) {
    if (tiConf.isTxnKVMode()) {
      ImportSstpb.WriteBatch batch;

      batch =
          ImportSstpb.WriteBatch.newBuilder()
              .addAllPairs(pairs)
              .setCommitTs(tiSession.getTimestamp().getVersion())
              .build();

      ImportSstpb.WriteRequest request =
          ImportSstpb.WriteRequest.newBuilder().setBatch(batch).build();
      for (ImporterStoreClient client : clientList) {
        client.writeBatch(request);
      }
    } else {
      ImportSstpb.RawWriteBatch batch;

      if (ttl == null || ttl <= 0) {
        batch = ImportSstpb.RawWriteBatch.newBuilder().addAllPairs(pairs).build();
      } else {
        batch = ImportSstpb.RawWriteBatch.newBuilder().addAllPairs(pairs).setTtl(ttl).build();
      }

      ImportSstpb.RawWriteRequest request =
          ImportSstpb.RawWriteRequest.newBuilder().setBatch(batch).build();
      for (ImporterStoreClient client : clientList) {
        client.writeBatch(request);
      }
    }
  }

  private void finishWrite() {
    for (ImporterStoreClient client : clientList) {
      client.finishWrite();
    }
  }

  private void ingest() throws GrpcException {
    List<ImporterStoreClient> workingClients = new ArrayList<>(clientList);
    while (!workingClients.isEmpty()) {
      Iterator<ImporterStoreClient> itor = workingClients.iterator();
      while (itor.hasNext()) {
        ImporterStoreClient client = itor.next();
        if (client.isWriteResponseReceived()) {
          itor.remove();
        } else if (client.hasWriteResponseError()) {
          throw new GrpcException(client.getWriteError());
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

    Object writeResponse = clientLeader.getWriteResponse();
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.INGEST_BACKOFF);
    ingestWithRetry(writeResponse, backOffer);
  }

  private void ingestWithRetry(Object writeResponse, BackOffer backOffer) {
    try {
      clientLeader.multiIngest(region.getLeaderContext(), writeResponse);
    } catch (RegionException e) {
      logger.warn("ingest failed.", e);
      boolean retry = false;
      Error error = e.getRegionErr();
      if (error != null) {
        if (error.hasNotLeader()) {
          retry = true;
          long newStoreId = error.getNotLeader().getLeader().getStoreId();

          // update Leader here
          logger.warn(
              String.format(
                  "NotLeader Error with region id %d and store id %d, new store id %d",
                  region.getId(), region.getLeader().getStoreId(), newStoreId));

          BackOffFunction.BackOffFuncType backOffFuncType;
          if (newStoreId != NO_LEADER_STORE_ID) {
            long regionId = region.getId();
            region = tiSession.getRegionManager().updateLeader(region, newStoreId);
            if (region == null) {
              // epoch is not changed, getRegionById is faster than getRegionByKey
              region = tiSession.getRegionManager().getRegionById(regionId);
            }
            backOffFuncType = BackOffFunction.BackOffFuncType.BoUpdateLeader;
          } else {
            logger.info(
                String.format(
                    "Received zero store id, from region %d try next time", region.getId()));
            tiSession.getRegionManager().invalidateRegion(region);
            region = tiSession.getRegionManager().getRegionById(region.getId());
            backOffFuncType = BackOffFunction.BackOffFuncType.BoRegionMiss;
          }

          backOffer.doBackOff(backOffFuncType, e);
          init();
        } else if (error.hasServerIsBusy()) {
          retry = true;
          // this error is reported from kv:
          // will occur when write pressure is high. Please try later.
          logger.warn(
              String.format(
                  "Server is busy for region [%s], reason: %s",
                  region, error.getServerIsBusy().getReason()));
          backOffer.doBackOff(
              BackOffFunction.BackOffFuncType.BoServerBusy,
              new StatusRuntimeException(
                  Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString())));
        } else {
          tiSession.getRegionManager().invalidateRegion(region);
        }
      }

      if (retry) {
        ingestWithRetry(writeResponse, backOffer);
      } else {
        throw e;
      }
    }
  }
}
