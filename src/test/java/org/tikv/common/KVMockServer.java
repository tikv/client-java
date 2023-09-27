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

import static org.tikv.common.key.Key.toRawKey;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.*;
import org.tikv.kvproto.Errorpb.EpochNotMatch;
import org.tikv.kvproto.Errorpb.Error;
import org.tikv.kvproto.Kvrpcpb.Context;

public class KVMockServer extends TikvGrpc.TikvImplBase {

  private static final Logger logger = LoggerFactory.getLogger(KVMockServer.class);
  private int port;
  private Server server;
  private TiRegion region;
  private TiRegion firstRegion;
  private Iterable<Metapb.Region> subregions;
  private Metapb.Peer newLeader;
  private Errorpb.StoreNotMatch storeNotMatch;
  private boolean tempError = false;
  private State state = State.Normal;
  private final TreeMap<Key, ByteString> dataMap = new TreeMap<>();
  private final Map<Key, Supplier<Errorpb.Error.Builder>> regionErrMap = new HashMap<>();

  private final Map<Key, Supplier<Kvrpcpb.KeyError.Builder>> keyErrMap = new HashMap<>();

  // for KV error
  public static final int ABORT = 1;
  public static final int RETRY = 2;
  // for raw client error
  public static final int NOT_LEADER = 3;
  public static final int REGION_NOT_FOUND = 4;
  public static final int KEY_NOT_IN_REGION = 5;
  public static final int STALE_EPOCH = 6;
  public static final int SERVER_IS_BUSY = 7;
  public static final int STALE_COMMAND = 8;
  public static final int STORE_NOT_MATCH = 9;
  public static final int RAFT_ENTRY_TOO_LARGE = 10;

  public enum State {
    Normal,
    Fail
  }

  public void setState(State state) {
    this.state = state;
  }

  public int getPort() {
    return port;
  }

  public void setRegion(TiRegion region) {
    this.region = region;
  }

  public void setNewLeader(Metapb.Peer leader) {
    newLeader = leader;
  }

  public void setSubregions(Iterable<Metapb.Region> subregions) {
    this.subregions = subregions;
  }

  public void setTempError(boolean tempError) {
    this.tempError = tempError;
  }

  public void setStoreNotMatch(int request, int actual) {
    this.storeNotMatch =
        Errorpb.StoreNotMatch.newBuilder()
            .setRequestStoreId(request)
            .setActualStoreId(actual)
            .build();
  }

  public void put(ByteString key, ByteString value) {
    dataMap.put(toRawKey(key), value);
  }

  public void remove(ByteString key) {
    dataMap.remove(toRawKey(key));
  }

  public void put(String key, String value) {
    put(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value));
  }

  public void put(String key, ByteString data) {
    put(ByteString.copyFromUtf8(key), data);
  }

  public void putError(String key, Supplier<Errorpb.Error.Builder> builder) {
    regionErrMap.put(toRawKey(key.getBytes(StandardCharsets.UTF_8)), builder);
  }

  private Supplier<Errorpb.Error.Builder> takeError(Key key, boolean tempError) {
    if (tempError) {
      return regionErrMap.remove(key);
    } else {
      return regionErrMap.get(key);
    }
  }

  public void clearAllMap() {
    dataMap.clear();
    regionErrMap.clear();
  }

  public void reset() {
    clearAllMap();
    state = State.Normal;
    storeNotMatch = null;
    newLeader = null;
    subregions = null;
    region = firstRegion;
    tempError = false;
  }

  private <T> boolean tryBuilderRegionError(Key key, T builder) throws Exception {
    Supplier<Errorpb.Error.Builder> errProvider = takeError(key, tempError);
    if (errProvider != null) {
      Error.Builder eb = errProvider.get();
      if (eb != null) {
        builder
            .getClass()
            .getMethod("setRegionError", new Class<?>[]{Error.class})
            .invoke(builder, eb.build());
      }
      return true;
    }
    return false;
  }

  private Errorpb.Error verifyContext(Context context) throws Exception {
    if (context.getRegionId() != region.getId()) {
      String errMsg =
          String.format(
              "client context mismatch: server: %s; client: %s",
              region.toString(), context.toString());
      throw new Exception(errMsg);
    }

    logger.warn("local region: " + region.toString());
    logger.warn("client context: " + context);

    Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
    if (storeNotMatch != null) {
      return errBuilder.setStoreNotMatch(storeNotMatch).build();
    }
    if (!context.getPeer().equals(region.getLeader())) {
      String warnMsg =
          String.format("this store %d is not leader, new leader: %s", port, newLeader.toString());
      logger.warn(warnMsg);
      return errBuilder
          .setNotLeader(
              Errorpb.NotLeader.newBuilder()
                  .setRegionId(context.getRegionId())
                  .setLeader(newLeader)
                  .build())
          .build();
    }

    if (!context.getRegionEpoch().equals(region.getRegionEpoch())) {
      return errBuilder
          .setEpochNotMatch(EpochNotMatch.newBuilder().addAllCurrentRegions(subregions).build())
          .build();
    }

    return null;
  }

  @Override
  public void rawGet(
      org.tikv.kvproto.Kvrpcpb.RawGetRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.RawGetResponse> responseObserver) {
    try {
      switch (state) {
        case Fail:
          throw new Exception(State.Fail.toString());
        default:
      }
      Key key = toRawKey(request.getKey());
      Kvrpcpb.RawGetResponse.Builder builder = Kvrpcpb.RawGetResponse.newBuilder();

      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      if (!tryBuilderRegionError(key, builder)) {
        ByteString value = dataMap.get(key);
        if (value == null) {
          value = ByteString.EMPTY;
        }
        builder.setValue(value);
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("internal error", e);
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void rawPut(
      org.tikv.kvproto.Kvrpcpb.RawPutRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.RawPutResponse> responseObserver) {
    try {
      Key key = toRawKey(request.getKey());
      Kvrpcpb.RawPutResponse.Builder builder = Kvrpcpb.RawPutResponse.newBuilder();

      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      Supplier<Errorpb.Error.Builder> errProvider = regionErrMap.get(key);
      if (errProvider != null) {
        Error.Builder eb = errProvider.get();
        if (eb != null) {
          builder.setRegionError(eb.build());
        }
      }

      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void rawDelete(
      org.tikv.kvproto.Kvrpcpb.RawDeleteRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.RawDeleteResponse> responseObserver) {
    try {
      Key key = toRawKey(request.getKey());
      Kvrpcpb.RawDeleteResponse.Builder builder = Kvrpcpb.RawDeleteResponse.newBuilder();

      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      Supplier<Errorpb.Error.Builder> errProvider = regionErrMap.get(key);
      if (errProvider != null) {
        Error.Builder eb = errProvider.get();
        if (eb != null) {
          builder.setRegionError(eb.build());
        }
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void kvGet(
      org.tikv.kvproto.Kvrpcpb.GetRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.GetResponse> responseObserver) {
    try {
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      Key key = toRawKey(request.getKey());
      Kvrpcpb.GetResponse.Builder builder = Kvrpcpb.GetResponse.newBuilder();

      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      Supplier<Kvrpcpb.KeyError.Builder> errProvider = keyErrMap.remove(key);
      if (errProvider != null) {
        builder.setError(errProvider.get().build());
      } else {
        ByteString value = dataMap.get(key);
        builder.setValue(value);
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void kvScan(
      org.tikv.kvproto.Kvrpcpb.ScanRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.ScanResponse> responseObserver) {
    try {
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      Key key = toRawKey(request.getStartKey());
      Kvrpcpb.ScanResponse.Builder builder = Kvrpcpb.ScanResponse.newBuilder();

      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      Supplier<Errorpb.Error.Builder> errProvider = regionErrMap.get(key);
      if (errProvider != null) {
        Error.Builder eb = errProvider.get();
        if (eb != null) {
          builder.setRegionError(eb.build());
        }
      } else {
        SortedMap<Key, ByteString> kvs = dataMap.tailMap(key);
        builder.addAllPairs(
            kvs.entrySet()
                .stream()
                .map(
                    kv ->
                        Kvrpcpb.KvPair.newBuilder()
                            .setKey(kv.getKey().toByteString())
                            .setValue(kv.getValue())
                            .build())
                .collect(Collectors.toList()));
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void kvBatchGet(
      org.tikv.kvproto.Kvrpcpb.BatchGetRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.BatchGetResponse> responseObserver) {
    try {
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      List<ByteString> keys = request.getKeysList();

      Kvrpcpb.BatchGetResponse.Builder builder = Kvrpcpb.BatchGetResponse.newBuilder();
      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      ImmutableList.Builder<Kvrpcpb.KvPair> resultList = ImmutableList.builder();
      for (ByteString key : keys) {
        Key rawKey = toRawKey(key);
        Supplier<Errorpb.Error.Builder> errProvider = regionErrMap.get(rawKey);
        if (errProvider != null) {
          Error.Builder eb = errProvider.get();
          if (eb != null) {
            builder.setRegionError(eb.build());
            break;
          }
        }

        ByteString value = dataMap.get(rawKey);
        resultList.add(Kvrpcpb.KvPair.newBuilder().setKey(key).setValue(value).build());
      }
      builder.addAllPairs(resultList.build());
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void coprocessor(
      org.tikv.kvproto.Coprocessor.Request requestWrap,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Coprocessor.Response> responseObserver) {
    try {
      DAGRequest request = DAGRequest.parseFrom(requestWrap.getData());
      if (request.getStartTsFallback() == 0) {
        throw new Exception();
      }

      List<Coprocessor.KeyRange> keyRanges = requestWrap.getRangesList();

      Coprocessor.Response.Builder builderWrap = Coprocessor.Response.newBuilder();
      Error e = verifyContext(requestWrap.getContext());
      if (e != null) {
        responseObserver.onNext(builderWrap.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      SelectResponse.Builder builder = SelectResponse.newBuilder();
      for (Coprocessor.KeyRange keyRange : keyRanges) {
        Key startKey = toRawKey(keyRange.getStart());
        Supplier<Errorpb.Error.Builder> errProvider = regionErrMap.get(startKey);
        if (errProvider != null) {
          Error.Builder eb = errProvider.get();
          if (eb != null) {
            builderWrap.setRegionError(eb.build());
            break;
          }
        }

        SortedMap<Key, ByteString> kvs = dataMap.tailMap(startKey);
        builder.addAllChunks(
            kvs.entrySet()
                .stream()
                .filter(Objects::nonNull)
                .filter(kv -> kv.getKey().compareTo(toRawKey(keyRange.getEnd())) <= 0)
                .map(kv -> Chunk.newBuilder().setRowsData(kv.getValue()).build())
                .collect(Collectors.toList()));
      }

      responseObserver.onNext(builderWrap.setData(builder.build().toByteString()).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public int start(TiRegion region) throws IOException {
    int port;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    start(region, port);
    return port;
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

  public void start(TiRegion region, int port) throws IOException {
    this.port = port;
    this.region = region;
    this.firstRegion = region;

    logger.info("start mock server on port: " + port);
    server =
        ServerBuilder.forPort(port).addService(new HealCheck()).addService(this).build().start();
    Runtime.getRuntime().addShutdownHook(new Thread(KVMockServer.this::stop));
  }

  public void stop() {
    if (server != null) {
      server.shutdownNow();
      try {
        server.awaitTermination();
      } catch (Exception ignore) {

      }
    }
  }
}
