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
import org.tikv.common.operation.NoopHandler;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.ImportSSTGrpc;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Kvrpcpb;

public class ImporterStoreClient
    extends AbstractGRPCClient<ImportSSTGrpc.ImportSSTBlockingStub, ImportSSTGrpc.ImportSSTStub>
    implements StreamObserver<ImportSstpb.RawWriteResponse> {

  private static final Logger logger = LoggerFactory.getLogger(ImporterStoreClient.class);

  protected ImporterStoreClient(
      TiConfiguration conf,
      ChannelFactory channelFactory,
      ImportSSTGrpc.ImportSSTBlockingStub blockingStub,
      ImportSSTGrpc.ImportSSTStub asyncStub) {
    super(conf, channelFactory, blockingStub, asyncStub);
  }

  private StreamObserver<ImportSstpb.RawWriteRequest> streamObserverRequest;
  private ImportSstpb.RawWriteResponse rawWriteResponse;
  private Throwable rawWriteError;

  public synchronized boolean isRawWriteResponseReceived() {
    return rawWriteResponse != null;
  }

  private synchronized ImportSstpb.RawWriteResponse getRawWriteResponse() {
    return rawWriteResponse;
  }

  private synchronized void setRawWriteResponse(ImportSstpb.RawWriteResponse rawWriteResponse) {
    this.rawWriteResponse = rawWriteResponse;
  }

  public synchronized boolean hasRawWriteResponseError() {
    return this.rawWriteError != null;
  }

  public synchronized Throwable getRawWriteError() {
    return this.rawWriteError;
  }

  private synchronized void setRawWriteError(Throwable t) {
    this.rawWriteError = t;
  }

  @Override
  public void onNext(ImportSstpb.RawWriteResponse value) {
    setRawWriteResponse(value);
  }

  @Override
  public void onError(Throwable t) {
    setRawWriteError(t);
    logger.error("Error during raw write!", t);
  }

  @Override
  public void onCompleted() {
    // do nothing
  }

  /**
   * Ingest KV pairs to RawKV using gRPC streaming mode. This API should be called on both leader
   * and followers.
   *
   * @return
   */
  public void startRawWrite() {
    streamObserverRequest = getAsyncStub().rawWrite(this);
  }

  /**
   * This API should be called after `startRawWrite`.
   *
   * @param request
   */
  public void rawWriteBatch(ImportSstpb.RawWriteRequest request) {
    streamObserverRequest.onNext(request);
  }

  /** This API should be called after `rawWriteBatch`. */
  public void finishRawWrite() {
    streamObserverRequest.onCompleted();
  }

  /**
   * This API should be called after `finishRawWrite`. This API should be called on leader only.
   *
   * @param ctx
   */
  public void multiIngest(Kvrpcpb.Context ctx) {
    List<ImportSstpb.SSTMeta> metasList = getRawWriteResponse().getMetasList();

    ImportSstpb.MultiIngestRequest request =
        ImportSstpb.MultiIngestRequest.newBuilder().setContext(ctx).addAllSsts(metasList).build();

    ImportSstpb.IngestResponse response = getBlockingStub().multiIngest(request);
    if (response.hasError()) {
      throw new GrpcException("" + response.getError());
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
    return blockingStub.withDeadlineAfter(conf.getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected ImportSSTGrpc.ImportSSTStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(conf.getIngestTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws Exception {}

  public static class ImporterStoreClientBuilder {
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
      logger.debug(String.format("Create region store client on address %s", addressStr));

      ManagedChannel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
      ImportSSTGrpc.ImportSSTBlockingStub blockingStub = ImportSSTGrpc.newBlockingStub(channel);
      ImportSSTGrpc.ImportSSTStub asyncStub = ImportSSTGrpc.newStub(channel);

      return new ImporterStoreClient(conf, channelFactory, blockingStub, asyncStub);
    }
  }
}
