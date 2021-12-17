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
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.RegionException;
import org.tikv.common.operation.NoopHandler;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.ImportSSTGrpc;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Kvrpcpb;

public class ImporterStoreClient<RequestClass, ResponseClass>
    extends AbstractGRPCClient<
        ImportSSTGrpc.ImportSSTBlockingStub, ImportSSTGrpc.ImportSSTFutureStub>
    implements StreamObserver<ResponseClass> {

  private static final Logger logger = LoggerFactory.getLogger(ImporterStoreClient.class);

  private final ImportSSTGrpc.ImportSSTStub stub;

  protected ImporterStoreClient(
      TiConfiguration conf,
      ChannelFactory channelFactory,
      ImportSSTGrpc.ImportSSTBlockingStub blockingStub,
      ImportSSTGrpc.ImportSSTFutureStub asyncStub,
      ImportSSTGrpc.ImportSSTStub stub) {
    super(conf, channelFactory, blockingStub, asyncStub);
    this.stub = stub;
  }

  private StreamObserver<RequestClass> streamObserverRequest;
  private ResponseClass writeResponse;
  private Throwable writeError;

  public synchronized boolean isWriteResponseReceived() {
    return writeResponse != null;
  }

  public synchronized ResponseClass getWriteResponse() {
    return writeResponse;
  }

  private synchronized void setWriteResponse(ResponseClass writeResponse) {
    this.writeResponse = writeResponse;
  }

  public synchronized boolean hasWriteResponseError() {
    return this.writeError != null;
  }

  public synchronized Throwable getWriteError() {
    return this.writeError;
  }

  private synchronized void setWriteError(Throwable t) {
    this.writeError = t;
  }

  @Override
  public void onNext(ResponseClass response) {
    setWriteResponse(response);
  }

  @Override
  public void onError(Throwable t) {
    setWriteError(t);
    logger.error("Error during write!", t);
  }

  @Override
  public void onCompleted() {
    // do nothing
  }

  /**
   * Ingest KV pairs to RawKV/Txn using gRPC streaming mode. This API should be called on both
   * leader and followers.
   *
   * @return
   */
  public void startWrite() {
    if (conf.isRawKVMode()) {
      streamObserverRequest =
          (StreamObserver<RequestClass>)
              getStub().rawWrite((StreamObserver<ImportSstpb.RawWriteResponse>) this);
    } else {
      streamObserverRequest =
          (StreamObserver<RequestClass>)
              getStub().write((StreamObserver<ImportSstpb.WriteResponse>) this);
    }
  }

  /**
   * This API should be called after `startWrite`.
   *
   * @param request
   */
  public void writeBatch(RequestClass request) {
    streamObserverRequest.onNext(request);
  }

  /** This API should be called after `writeBatch`. */
  public void finishWrite() {
    streamObserverRequest.onCompleted();
  }

  /**
   * This API should be called after `finishWrite`. This API should be called on leader only.
   *
   * @param ctx
   * @param writeResponse
   * @throws RegionException
   */
  public void multiIngest(Kvrpcpb.Context ctx, Object writeResponse) throws RegionException {
    List<ImportSstpb.SSTMeta> metasList;
    if (writeResponse instanceof ImportSstpb.RawWriteResponse) {
      metasList = ((ImportSstpb.RawWriteResponse) writeResponse).getMetasList();
    } else if (writeResponse instanceof ImportSstpb.WriteResponse) {
      metasList = ((ImportSstpb.WriteResponse) writeResponse).getMetasList();
    } else {
      throw new IllegalArgumentException("Wrong response type: " + writeResponse);
    }

    ImportSstpb.MultiIngestRequest request =
        ImportSstpb.MultiIngestRequest.newBuilder().setContext(ctx).addAllSsts(metasList).build();

    ImportSstpb.IngestResponse response = getBlockingStub().multiIngest(request);
    if (response.hasError()) {
      throw new RegionException(response.getError());
    }
  }

  public void switchMode(ImportSstpb.SwitchMode mode) {
    Supplier<ImportSstpb.SwitchModeRequest> request =
        () -> ImportSstpb.SwitchModeRequest.newBuilder().setMode(mode).build();
    NoopHandler<ImportSstpb.SwitchModeResponse> noopHandler = new NoopHandler<>();

    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(BackOffer.TIKV_SWITCH_MODE_BACKOFF),
        ImportSSTGrpc.getSwitchModeMethod(),
        request,
        noopHandler);
  }

  @Override
  protected ImportSSTGrpc.ImportSSTBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(conf.getIngestTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected ImportSSTGrpc.ImportSSTFutureStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(conf.getIngestTimeout(), TimeUnit.MILLISECONDS);
  }

  protected ImportSSTGrpc.ImportSSTStub getStub() {
    return stub.withDeadlineAfter(conf.getIngestTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws Exception {}

  public static class ImporterStoreClientBuilder<RequestClass, ResponseClass> {

    private final TiConfiguration conf;
    private final ChannelFactory channelFactory;
    private final RegionManager regionManager;
    private final PDClient pdClient;

    public ImporterStoreClientBuilder(
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

    public synchronized ImporterStoreClient build(TiStore store) throws GrpcException {
      Objects.requireNonNull(store, "store is null");

      String addressStr = store.getStore().getAddress();
      logger.atDebug().log("Create region store client on address {}", addressStr);

      ManagedChannel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
      ImportSSTGrpc.ImportSSTBlockingStub blockingStub = ImportSSTGrpc.newBlockingStub(channel);
      ImportSSTGrpc.ImportSSTFutureStub asyncStub = ImportSSTGrpc.newFutureStub(channel);
      ImportSSTGrpc.ImportSSTStub stub = ImportSSTGrpc.newStub(channel);

      return new ImporterStoreClient<RequestClass, ResponseClass>(
          conf, channelFactory, blockingStub, asyncStub, stub);
    }
  }
}
