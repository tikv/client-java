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

package org.tikv.common;

import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;

import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.AbstractFutureStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.operation.ErrorHandler;
import org.tikv.common.policy.RetryMaxMs.Builder;
import org.tikv.common.policy.RetryPolicy;
import org.tikv.common.streaming.StreamingResponse;
import org.tikv.common.util.BackOffFunction.BackOffFuncType;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ChannelFactory;

public abstract class AbstractGRPCClient<
        BlockingStubT extends AbstractStub<BlockingStubT>,
        FutureStubT extends AbstractFutureStub<FutureStubT>>
    implements AutoCloseable {
  protected final Logger logger = LoggerFactory.getLogger(this.getClass());
  protected final ChannelFactory channelFactory;
  protected TiConfiguration conf;
  protected long timeout;
  protected BlockingStubT blockingStub;
  protected FutureStubT asyncStub;

  protected AbstractGRPCClient(TiConfiguration conf, ChannelFactory channelFactory) {
    this.conf = conf;
    this.timeout = conf.getTimeout();
    this.channelFactory = channelFactory;
  }

  protected AbstractGRPCClient(
      TiConfiguration conf,
      ChannelFactory channelFactory,
      BlockingStubT blockingStub,
      FutureStubT asyncStub) {
    this.conf = conf;
    this.timeout = conf.getTimeout();
    this.channelFactory = channelFactory;
    this.blockingStub = blockingStub;
    this.asyncStub = asyncStub;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  // TODO: Seems a little bit messy for lambda part
  public <ReqT, RespT> RespT callWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      Supplier<ReqT> requestFactory,
      ErrorHandler<RespT> handler) {
    if (logger.isTraceEnabled()) {
      logger.trace(String.format("Calling %s...", method.getFullMethodName()));
    }
    RetryPolicy<RespT> policy = new Builder<RespT>(backOffer).create(handler);
    RespT resp =
        policy.callWithRetry(
            () -> {
              BlockingStubT stub = getBlockingStub();
              return ClientCalls.blockingUnaryCall(
                  stub.getChannel(), method, stub.getCallOptions(), requestFactory.get());
            },
            method.getFullMethodName(),
            backOffer);

    if (logger.isTraceEnabled()) {
      logger.trace(String.format("leaving %s...", method.getFullMethodName()));
    }
    return resp;
  }

  protected <ReqT, RespT> void callAsyncWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      Supplier<ReqT> requestFactory,
      StreamObserver<RespT> responseObserver,
      ErrorHandler<RespT> handler) {
    logger.debug(String.format("Calling %s...", method.getFullMethodName()));

    RetryPolicy<RespT> policy = new Builder<RespT>(backOffer).create(handler);
    policy.callWithRetry(
        () -> {
          FutureStubT stub = getAsyncStub();
          ClientCalls.asyncUnaryCall(
              stub.getChannel().newCall(method, stub.getCallOptions()),
              requestFactory.get(),
              responseObserver);
          return null;
        },
        method.getFullMethodName(),
        backOffer);
    logger.debug(String.format("leaving %s...", method.getFullMethodName()));
  }

  <ReqT, RespT> StreamObserver<ReqT> callBidiStreamingWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      StreamObserver<RespT> responseObserver,
      ErrorHandler<StreamObserver<ReqT>> handler) {
    logger.debug(String.format("Calling %s...", method.getFullMethodName()));

    RetryPolicy<StreamObserver<ReqT>> policy =
        new Builder<StreamObserver<ReqT>>(backOffer).create(handler);
    StreamObserver<ReqT> observer =
        policy.callWithRetry(
            () -> {
              FutureStubT stub = getAsyncStub();
              return asyncBidiStreamingCall(
                  stub.getChannel().newCall(method, stub.getCallOptions()), responseObserver);
            },
            method.getFullMethodName(),
            backOffer);
    logger.debug(String.format("leaving %s...", method.getFullMethodName()));
    return observer;
  }

  public <ReqT, RespT> StreamingResponse callServerStreamingWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      Supplier<ReqT> requestFactory,
      ErrorHandler<StreamingResponse> handler) {
    logger.debug(String.format("Calling %s...", method.getFullMethodName()));

    RetryPolicy<StreamingResponse> policy =
        new Builder<StreamingResponse>(backOffer).create(handler);
    StreamingResponse response =
        policy.callWithRetry(
            () -> {
              BlockingStubT stub = getBlockingStub();
              return new StreamingResponse(
                  blockingServerStreamingCall(
                      stub.getChannel(), method, stub.getCallOptions(), requestFactory.get()));
            },
            method.getFullMethodName(),
            backOffer);
    logger.debug(String.format("leaving %s...", method.getFullMethodName()));
    return response;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public long getTimeout() {
    return this.timeout;
  }

  protected abstract BlockingStubT getBlockingStub();

  protected abstract FutureStubT getAsyncStub();

  private boolean doCheckHealth(BackOffer backOffer, String addressStr, HostMapping hostMapping) {
    while (true) {
      backOffer.checkTimeout();

      try {
        ManagedChannel channel = channelFactory.getChannel(addressStr, hostMapping);
        HealthGrpc.HealthBlockingStub stub =
            HealthGrpc.newBlockingStub(channel)
                .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
        HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
        HealthCheckResponse resp = stub.check(req);
        return resp.getStatus() == HealthCheckResponse.ServingStatus.SERVING;
      } catch (Exception e) {
        logger.warn("check health failed.", e);
        backOffer.doBackOff(BackOffFuncType.BoCheckHealth, e);
      }
    }
  }

  protected boolean checkHealth(BackOffer backOffer, String addressStr, HostMapping hostMapping) {
    return doCheckHealth(backOffer, addressStr, hostMapping);
  }
}
