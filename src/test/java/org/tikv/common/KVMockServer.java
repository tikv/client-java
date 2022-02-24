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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Errorpb.EpochNotMatch;
import org.tikv.kvproto.Errorpb.Error;
import org.tikv.kvproto.Errorpb.NotLeader;
import org.tikv.kvproto.Errorpb.ServerIsBusy;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.Context;
import org.tikv.kvproto.TikvGrpc;

public class KVMockServer extends TikvGrpc.TikvImplBase {

  private static final Logger logger = LoggerFactory.getLogger(KVMockServer.class);
  private int port;
  private Server server;
  private TiRegion region;
  private State state = State.Normal;
  private final TreeMap<Key, ByteString> dataMap = new TreeMap<>();
  private final Map<ByteString, Integer> errorMap = new HashMap<>();

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

  public void putError(String key, int code) {
    errorMap.put(ByteString.copyFromUtf8(key), code);
  }

  public void clearAllMap() {
    dataMap.clear();
    errorMap.clear();
  }

  private void verifyContext(Context context) throws Exception {
    if (context.getRegionId() != region.getId()
        || !context.getRegionEpoch().equals(region.getRegionEpoch())
        || !context.getPeer().equals(region.getLeader())) {
      throw new Exception("context doesn't match");
    }
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
      verifyContext(request.getContext());
      ByteString key = request.getKey();

      Kvrpcpb.RawGetResponse.Builder builder = Kvrpcpb.RawGetResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
      if (errorCode != null) {
        setErrorInfo(errorCode, errBuilder);
        builder.setRegionError(errBuilder.build());
      } else {
        Key rawKey = toRawKey(key);
        ByteString value = dataMap.get(rawKey);
        if (value == null) {
          value = ByteString.EMPTY;
        }
        builder.setValue(value);
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  /**
   *
   */
  public void rawPut(
      org.tikv.kvproto.Kvrpcpb.RawPutRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.RawPutResponse> responseObserver) {
    try {
      verifyContext(request.getContext());
      ByteString key = request.getKey();

      Kvrpcpb.RawPutResponse.Builder builder = Kvrpcpb.RawPutResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
      if (errorCode != null) {
        setErrorInfo(errorCode, errBuilder);
        builder.setRegionError(errBuilder.build());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  private void setErrorInfo(int errorCode, Errorpb.Error.Builder errBuilder) {
    if (errorCode == NOT_LEADER) {
      errBuilder.setNotLeader(Errorpb.NotLeader.getDefaultInstance());
    } else if (errorCode == REGION_NOT_FOUND) {
      errBuilder.setRegionNotFound(Errorpb.RegionNotFound.getDefaultInstance());
    } else if (errorCode == KEY_NOT_IN_REGION) {
      errBuilder.setKeyNotInRegion(Errorpb.KeyNotInRegion.getDefaultInstance());
    } else if (errorCode == STALE_EPOCH) {
      errBuilder.setEpochNotMatch(Errorpb.EpochNotMatch.getDefaultInstance());
    } else if (errorCode == STALE_COMMAND) {
      errBuilder.setStaleCommand(Errorpb.StaleCommand.getDefaultInstance());
    } else if (errorCode == SERVER_IS_BUSY) {
      errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
    } else if (errorCode == STORE_NOT_MATCH) {
      errBuilder.setStoreNotMatch(Errorpb.StoreNotMatch.getDefaultInstance());
    } else if (errorCode == RAFT_ENTRY_TOO_LARGE) {
      errBuilder.setRaftEntryTooLarge(Errorpb.RaftEntryTooLarge.getDefaultInstance());
    }
  }

  /**
   *
   */
  public void rawDelete(
      org.tikv.kvproto.Kvrpcpb.RawDeleteRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.RawDeleteResponse> responseObserver) {
    try {
      verifyContext(request.getContext());
      ByteString key = request.getKey();

      Kvrpcpb.RawDeleteResponse.Builder builder = Kvrpcpb.RawDeleteResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
      if (errorCode != null) {
        setErrorInfo(errorCode, errBuilder);
        builder.setRegionError(errBuilder.build());
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
      verifyContext(request.getContext());
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      ByteString key = request.getKey();

      Kvrpcpb.GetResponse.Builder builder = Kvrpcpb.GetResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Kvrpcpb.KeyError.Builder errBuilder = Kvrpcpb.KeyError.newBuilder();
      if (errorCode != null) {
        if (errorCode == ABORT) {
          errBuilder.setAbort("ABORT");
        } else if (errorCode == RETRY) {
          errBuilder.setRetryable("Retry");
        }
        builder.setError(errBuilder);
      } else {
        ByteString value = dataMap.get(toRawKey(key));
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
      verifyContext(request.getContext());
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      ByteString key = request.getStartKey();

      Kvrpcpb.ScanResponse.Builder builder = Kvrpcpb.ScanResponse.newBuilder();
      Error.Builder errBuilder = Error.newBuilder();
      Integer errorCode = errorMap.remove(key);
      if (errorCode != null) {
        if (errorCode == ABORT) {
          errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
        }
        builder.setRegionError(errBuilder.build());
      } else {
        ByteString startKey = request.getStartKey();
        SortedMap<Key, ByteString> kvs = dataMap.tailMap(toRawKey(startKey));
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
      verifyContext(request.getContext());
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      List<ByteString> keys = request.getKeysList();

      Kvrpcpb.BatchGetResponse.Builder builder = Kvrpcpb.BatchGetResponse.newBuilder();
      Error.Builder errBuilder = Error.newBuilder();
      ImmutableList.Builder<Kvrpcpb.KvPair> resultList = ImmutableList.builder();
      for (ByteString key : keys) {
        Integer errorCode = errorMap.remove(key);
        if (errorCode != null) {
          if (errorCode == ABORT) {
            errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
          }
          builder.setRegionError(errBuilder.build());
          break;
        } else {
          ByteString value = dataMap.get(toRawKey(key));
          resultList.add(Kvrpcpb.KvPair.newBuilder().setKey(key).setValue(value).build());
        }
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
      verifyContext(requestWrap.getContext());

      DAGRequest request = DAGRequest.parseFrom(requestWrap.getData());
      if (request.getStartTsFallback() == 0) {
        throw new Exception();
      }

      List<Coprocessor.KeyRange> keyRanges = requestWrap.getRangesList();

      Coprocessor.Response.Builder builderWrap = Coprocessor.Response.newBuilder();
      SelectResponse.Builder builder = SelectResponse.newBuilder();
      org.tikv.kvproto.Errorpb.Error.Builder errBuilder =
          org.tikv.kvproto.Errorpb.Error.newBuilder();

      for (Coprocessor.KeyRange keyRange : keyRanges) {
        Integer errorCode = errorMap.remove(keyRange.getStart());
        if (errorCode != null) {
          if (STALE_EPOCH == errorCode) {
            errBuilder.setEpochNotMatch(EpochNotMatch.getDefaultInstance());
          } else if (NOT_LEADER == errorCode) {
            errBuilder.setNotLeader(NotLeader.getDefaultInstance());
          } else {
            errBuilder.setServerIsBusy(ServerIsBusy.getDefaultInstance());
          }
          builderWrap.setRegionError(errBuilder.build());
          break;
        } else {
          ByteString startKey = keyRange.getStart();
          SortedMap<Key, ByteString> kvs = dataMap.tailMap(toRawKey(startKey));
          builder.addAllChunks(
              kvs.entrySet()
                  .stream()
                  .filter(Objects::nonNull)
                  .filter(kv -> kv.getKey().compareTo(toRawKey(keyRange.getEnd())) <= 0)
                  .map(kv -> Chunk.newBuilder().setRowsData(kv.getValue()).build())
                  .collect(Collectors.toList()));
        }
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

    logger.info("start mock server on port: " + port);
    server =
        ServerBuilder.forPort(port).addService(new HealCheck()).addService(this).build().start();
    Runtime.getRuntime().addShutdownHook(new Thread(KVMockServer.this::stop));
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }
}
