/*
 * Copyright 2017 TiKV Project Authors.
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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.Pdpb.GetMembersRequest;
import org.tikv.kvproto.Pdpb.GetMembersResponse;
import org.tikv.kvproto.Pdpb.GetRegionByIDRequest;
import org.tikv.kvproto.Pdpb.GetRegionRequest;
import org.tikv.kvproto.Pdpb.GetRegionResponse;
import org.tikv.kvproto.Pdpb.GetStoreRequest;
import org.tikv.kvproto.Pdpb.GetStoreResponse;
import org.tikv.kvproto.Pdpb.TsoRequest;
import org.tikv.kvproto.Pdpb.TsoResponse;

public class PDMockServer extends PDGrpc.PDImplBase {

  public int port;
  private long clusterId;
  private Server server;

  private Interceptor<GetMembersRequest, GetMembersResponse> getMembersInterceptor;
  private Interceptor<GetStoreRequest, GetStoreResponse> getStoreInterceptor;
  private Interceptor<GetRegionRequest, GetRegionResponse> getRegionInterceptor;
  private Interceptor<GetRegionByIDRequest, GetRegionResponse> getRegionByIDInterceptor;

  public void addGetMembersInterceptor(
      Interceptor<GetMembersRequest, GetMembersResponse> interceptor) {
    getMembersInterceptor = interceptor;
  }

  @Override
  public void getMembers(GetMembersRequest request, StreamObserver<GetMembersResponse> resp) {
    try {
      resp.onNext(getMembersInterceptor.apply(request));
      resp.onCompleted();
    } catch (Throwable t) {
      resp.onError(t);
    }
  }

  @Override
  public StreamObserver<TsoRequest> tso(StreamObserver<TsoResponse> resp) {
    return new StreamObserver<TsoRequest>() {
      private int physical = 1;
      private int logical = 0;

      @Override
      public void onNext(TsoRequest value) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onCompleted() {
        resp.onNext(GrpcUtils.makeTsoResponse(clusterId, physical++, logical++));
        resp.onCompleted();
      }
    };
  }

  public void addGetRegionInterceptor(Interceptor<GetRegionRequest, GetRegionResponse> func) {
    getRegionInterceptor = func;
  }

  @Override
  public void getRegion(GetRegionRequest request, StreamObserver<GetRegionResponse> resp) {
    try {
      resp.onNext(getRegionInterceptor.apply(request));
      resp.onCompleted();
    } catch (Throwable t) {
      resp.onError(t);
    }
  }

  public void addGetRegionByIdInterceptor(
      Interceptor<GetRegionByIDRequest, GetRegionResponse> func) {
    getRegionByIDInterceptor = func;
  }

  @Override
  public void getRegionByID(GetRegionByIDRequest request, StreamObserver<GetRegionResponse> resp) {
    try {
      resp.onNext(getRegionByIDInterceptor.apply(request));
      resp.onCompleted();
    } catch (Throwable e) {
      resp.onError(e);
    }
  }

  public void addGetStoreInterceptor(Interceptor<GetStoreRequest, GetStoreResponse> func) {
    getStoreInterceptor = func;
  }

  @Override
  public void getStore(GetStoreRequest request, StreamObserver<GetStoreResponse> resp) {
    try {
      resp.onNext(getStoreInterceptor.apply(request));
      resp.onCompleted();
    } catch (Throwable e) {
      resp.onError(e);
    }
  }

  public void start(long clusterId) throws IOException {
    int port;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    start(clusterId, port);
  }

  public void start(long clusterId, int port) throws IOException {
    this.clusterId = clusterId;
    this.port = port;
    server = ServerBuilder.forPort(port).addService(this).build().start();

    Runtime.getRuntime().addShutdownHook(new Thread(PDMockServer.this::stop));
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  public long getClusterId() {
    return clusterId;
  }
}
