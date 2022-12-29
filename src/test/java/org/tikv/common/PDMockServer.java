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
import io.grpc.Status;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc.HealthImplBase;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.function.Function;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.Pdpb.GetAllStoresRequest;
import org.tikv.kvproto.Pdpb.GetAllStoresResponse;
import org.tikv.kvproto.Pdpb.GetMembersRequest;
import org.tikv.kvproto.Pdpb.GetMembersResponse;
import org.tikv.kvproto.Pdpb.GetRegionByIDRequest;
import org.tikv.kvproto.Pdpb.GetRegionRequest;
import org.tikv.kvproto.Pdpb.GetRegionResponse;
import org.tikv.kvproto.Pdpb.GetStoreRequest;
import org.tikv.kvproto.Pdpb.GetStoreResponse;
import org.tikv.kvproto.Pdpb.ScanRegionsRequest;
import org.tikv.kvproto.Pdpb.ScanRegionsResponse;
import org.tikv.kvproto.Pdpb.TsoRequest;
import org.tikv.kvproto.Pdpb.TsoResponse;

public class PDMockServer extends PDGrpc.PDImplBase {
  private int port;
  private long clusterId;
  private Server server;

  private Function<GetMembersRequest, GetMembersResponse> getMembersListener;
  private Function<GetStoreRequest, GetStoreResponse> getStoreListener;
  private Function<GetRegionRequest, GetRegionResponse> getRegionListener;
  private Function<GetRegionByIDRequest, GetRegionResponse> getRegionByIDListener;

  private Function<ScanRegionsRequest, ScanRegionsResponse> scanRegionsListener;

  private Function<GetAllStoresRequest, GetAllStoresResponse> getAllStoresListener;

  public void addGetMembersListener(Function<GetMembersRequest, GetMembersResponse> func) {
    getMembersListener = func;
  }

  @Override
  public void getMembers(GetMembersRequest request, StreamObserver<GetMembersResponse> resp) {
    try {
      resp.onNext(Optional.ofNullable(getMembersListener.apply(request)).get());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public StreamObserver<TsoRequest> tso(StreamObserver<TsoResponse> resp) {
    return new StreamObserver<TsoRequest>() {
      private long physical = System.currentTimeMillis();
      private long logical = 0;

      private void updateTso() {
        logical++;
        if (logical >= (1 << 18)) {
          logical = 0;
          physical++;
        }
        physical = Math.max(physical, System.currentTimeMillis());
      }

      @Override
      public void onNext(TsoRequest value) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onCompleted() {
        updateTso();
        resp.onNext(GrpcUtils.makeTsoResponse(clusterId, physical, logical));
        resp.onCompleted();
      }
    };
  }

  public void addGetRegionListener(Function<GetRegionRequest, GetRegionResponse> func) {
    getRegionListener = func;
  }

  @Override
  public void getRegion(GetRegionRequest request, StreamObserver<GetRegionResponse> resp) {
    try {
      resp.onNext(getRegionListener.apply(request));
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public void addGetRegionByIDListener(Function<GetRegionByIDRequest, GetRegionResponse> func) {
    getRegionByIDListener = func;
  }

  @Override
  public void getRegionByID(GetRegionByIDRequest request, StreamObserver<GetRegionResponse> resp) {
    try {
      resp.onNext(getRegionByIDListener.apply(request));
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public void addGetStoreListener(Function<GetStoreRequest, GetStoreResponse> func) {
    getStoreListener = func;
  }

  @Override
  public void getStore(GetStoreRequest request, StreamObserver<GetStoreResponse> resp) {
    try {
      resp.onNext(Optional.ofNullable(getStoreListener.apply(request)).get());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public void addScanRegionsListener(Function<ScanRegionsRequest, ScanRegionsResponse> func) {
    scanRegionsListener = func;
  }

  @Override
  public void scanRegions(ScanRegionsRequest request, StreamObserver<ScanRegionsResponse> resp) {
    try {
      resp.onNext(Optional.ofNullable(scanRegionsListener.apply(request)).get());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public void addGetAllStoresListener(Function<GetAllStoresRequest, GetAllStoresResponse> func) {
    getAllStoresListener = func;
  }

  @Override
  public void getAllStores(GetAllStoresRequest request, StreamObserver<GetAllStoresResponse> resp) {
    try {
      resp.onNext(Optional.ofNullable(getAllStoresListener.apply(request)).get());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public void start(long clusterId) throws IOException {
    int port;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    start(clusterId, port);
  }

  private static class HealCheck extends HealthImplBase {
    @Override
    public void check(
        HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
      responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(ServingStatus.SERVING).build());
      responseObserver.onCompleted();
    }
  }

  public void start(long clusterId, int port) throws IOException {
    this.clusterId = clusterId;
    this.port = port;
    server =
        ServerBuilder.forPort(port).addService(new HealCheck()).addService(this).build().start();

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

  public long getPort() {
    return port;
  }
}
