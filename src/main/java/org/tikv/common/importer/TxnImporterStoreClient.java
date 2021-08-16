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

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.ChannelFactory;
import org.tikv.kvproto.ImportSSTGrpc;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Kvrpcpb;

public class TxnImporterStoreClient
    extends AbstractGRPCClient<ImportSSTGrpc.ImportSSTBlockingStub, ImportSSTGrpc.ImportSSTStub>
    implements StreamObserver<ImportSstpb.WriteResponse> {

  private static final Logger logger = LoggerFactory.getLogger(TxnImporterStoreClient.class);

  protected TxnImporterStoreClient(
      TiConfiguration conf,
      ChannelFactory channelFactory,
      ImportSSTGrpc.ImportSSTBlockingStub blockingStub,
      ImportSSTGrpc.ImportSSTStub asyncStub) {
    super(conf, channelFactory, blockingStub, asyncStub);
  }

  private StreamObserver<ImportSstpb.WriteRequest> streamObserverRequest;
  private ImportSstpb.WriteResponse txnWriteResponse;
  private Throwable txnWriteError;

  public synchronized boolean isTxnWriteResponseReceived() {
    return txnWriteResponse != null;
  }

  private synchronized ImportSstpb.WriteResponse getTxnWriteResponse() {
    return txnWriteResponse;
  }

  private synchronized void setTxnWriteResponse(ImportSstpb.WriteResponse txnWriteResponse) {
    this.txnWriteResponse = txnWriteResponse;
  }

  public synchronized boolean hasTxnWriteResponseError() {
    return this.txnWriteResponse != null;
  }

  public synchronized Throwable getTxnWriteError() {
    return this.txnWriteError;
  }

  private synchronized void setTxnWriteError(Throwable t) {
    this.txnWriteError = t;
  }

  @Override
  public void onNext(ImportSstpb.WriteResponse writeResponse) {
    setTxnWriteResponse(writeResponse);
  }

  @Override
  public void onError(Throwable throwable) {
    setTxnWriteError(throwable);
    logger.error("Error during txn write!", throwable);
  }

  @Override
  public void onCompleted() {
    // do nothing
  }

  @Override
  public void close() throws Exception {}

  public void startTxnWrite() {
    streamObserverRequest = getAsyncStub().write(this);
  }

  public void txnWriteBatch(ImportSstpb.WriteRequest request) {
    streamObserverRequest.onNext(request);
  }

  public void finishTxnWrite() {
    streamObserverRequest.onCompleted();
  }

  public void multiIngest(Kvrpcpb.Context ctx) {
    List<ImportSstpb.SSTMeta> metasList = getTxnWriteResponse().getMetasList();
    logger.info(metasList.get(0).getCfName());

    ImportSstpb.MultiIngestRequest request =
        ImportSstpb.MultiIngestRequest.newBuilder().setContext(ctx).addAllSsts(metasList).build();

    ImportSstpb.IngestResponse response = getBlockingStub().multiIngest(request);
    if (response.hasError()) {
      throw new GrpcException("" + response.getError());
    }
  }

  public static class TxnImporterStoreClientBuilder {
    private final TiConfiguration conf;
    private final ChannelFactory channelFactory;
    private final RegionManager regionManager;
    private final PDClient pdClient;

    public TxnImporterStoreClientBuilder(
        TiConfiguration conf,
        ChannelFactory channelFactory,
        RegionManager regionManager,
        PDClient pdClient) {
      Objects.requireNonNull(conf, "conf is null");
      Objects.requireNonNull(channelFactory, "channelFactory is null");
      Objects.requireNonNull(regionManager, "regionManager is null");
      this.conf = conf;
      this.channelFactory = channelFactory;
      this.regionManager = regionManager;
      this.pdClient = pdClient;
    }

    public synchronized TxnImporterStoreClient build(TiStore store) throws GrpcException {
      Objects.requireNonNull(store, "store is null");

      String addressStr = store.getStore().getAddress();
      logger.debug(String.format("Create region store client on address %s", addressStr));

      ManagedChannel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
      ImportSSTGrpc.ImportSSTBlockingStub blockingStub = ImportSSTGrpc.newBlockingStub(channel);
      ImportSSTGrpc.ImportSSTStub asyncStub = ImportSSTGrpc.newStub(channel);

      return new TxnImporterStoreClient(conf, channelFactory, blockingStub, asyncStub);
    }
  }

  @Override
  protected ImportSSTGrpc.ImportSSTBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected ImportSSTGrpc.ImportSSTStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(200, TimeUnit.SECONDS);
  }
}
