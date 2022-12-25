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
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Errorpb.EpochNotMatch;
import org.tikv.kvproto.Errorpb.Error;
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
  private final Map<Key, Supplier<Errorpb.Error.Builder>> regionErrMap = new HashMap<>();

  private final Map<Key, Supplier<Kvrpcpb.KeyError.Builder>> keyErrMap = new HashMap<>();

  private final Map<Key, Supplier<Kvrpcpb.LockInfo.Builder>> lockMap = new HashMap<>();
  private final Map<Long, Supplier<Kvrpcpb.CheckTxnStatusResponse.Builder>> txnStatusMap =
      new HashMap<>();

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

  public void putError(String key, Supplier<Errorpb.Error.Builder> builder) {
    regionErrMap.put(toRawKey(key.getBytes(StandardCharsets.UTF_8)), builder);
  }

  public void removeError(String key) {
    regionErrMap.remove(toRawKey(key.getBytes(StandardCharsets.UTF_8)));
  }

  // putWithLock is used to "prewrite" key-value without "commit"
  public void putWithLock(
      ByteString key, ByteString value, ByteString primaryKey, Long startTs, Long ttl) {
    put(key, value);

    Kvrpcpb.LockInfo.Builder lock =
        Kvrpcpb.LockInfo.newBuilder()
            .setPrimaryLock(primaryKey)
            .setLockVersion(startTs)
            .setKey(key)
            .setLockTtl(ttl);
    lockMap.put(toRawKey(key), () -> lock);
  }

  public void removeLock(ByteString key) {
    lockMap.remove(toRawKey(key));
  }

  public boolean hasLock(ByteString key) {
    return lockMap.containsKey(toRawKey(key));
  }

  // putTxnStatus is used to save transaction status
  // commitTs > 0: committed
  // commitTs == 0 && key is empty: rollback
  // commitTs == 0 && key not empty: locked by key
  public void putTxnStatus(Long startTs, Long commitTs, ByteString key) {
    if (commitTs > 0 || (commitTs == 0 && key.isEmpty())) { // committed || rollback
      Kvrpcpb.CheckTxnStatusResponse.Builder txnStatus =
          Kvrpcpb.CheckTxnStatusResponse.newBuilder()
              .setCommitVersion(commitTs)
              .setLockTtl(0)
              .setAction(Kvrpcpb.Action.NoAction);
      txnStatusMap.put(startTs, () -> txnStatus);
    } else { // locked
      Kvrpcpb.LockInfo.Builder lock = lockMap.get(toRawKey(key)).get();
      Kvrpcpb.CheckTxnStatusResponse.Builder txnStatus =
          Kvrpcpb.CheckTxnStatusResponse.newBuilder()
              .setCommitVersion(commitTs)
              .setLockTtl(lock.getLockTtl())
              .setAction(Kvrpcpb.Action.NoAction)
              .setLockInfo(lock);
      txnStatusMap.put(startTs, () -> txnStatus);
    }
  }

  // putTxnStatus is used to save transaction status
  // commitTs > 0: committed
  // commitTs == 0: rollback
  public void putTxnStatus(Long startTs, Long commitTs) {
    putTxnStatus(startTs, commitTs, ByteString.EMPTY);
  }

  public void clearAllMap() {
    dataMap.clear();
    regionErrMap.clear();
    lockMap.clear();
    txnStatusMap.clear();
  }

  private Errorpb.Error verifyContext(Context context) throws Exception {
    if (context.getRegionId() != region.getId() || !context.getPeer().equals(region.getLeader())) {
      throw new Exception("context doesn't match");
    }

    Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();

    if (!context.getRegionEpoch().equals(region.getRegionEpoch())) {
      return errBuilder
          .setEpochNotMatch(EpochNotMatch.newBuilder().addCurrentRegions(region.getMeta()).build())
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

      Supplier<Errorpb.Error.Builder> errProvider = regionErrMap.get(key);
      if (errProvider != null) {
        Error.Builder eb = errProvider.get();
        if (eb != null) {
          builder.setRegionError(eb.build());
        }
      } else {
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

      Supplier<Kvrpcpb.LockInfo.Builder> lock = lockMap.get(key);
      Supplier<Kvrpcpb.KeyError.Builder> errProvider = keyErrMap.remove(key);
      if (errProvider != null) {
        builder.setError(errProvider.get().build());
      } else if (lock != null) {
        builder.setError(Kvrpcpb.KeyError.newBuilder().setLocked(lock.get()));
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
                    kv -> {
                      Kvrpcpb.KvPair.Builder kvBuilder =
                          Kvrpcpb.KvPair.newBuilder()
                              .setKey(kv.getKey().toByteString())
                              .setValue(kv.getValue());
                      Supplier<Kvrpcpb.LockInfo.Builder> lock = lockMap.get(kv.getKey());
                      if (lock != null) {
                        kvBuilder.setError(Kvrpcpb.KeyError.newBuilder().setLocked(lock.get()));
                      }
                      return kvBuilder.build();
                    })
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
  public void kvCheckTxnStatus(
      org.tikv.kvproto.Kvrpcpb.CheckTxnStatusRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.CheckTxnStatusResponse>
          responseObserver) {
    logger.info("KVMockServer.kvCheckTxnStatus");
    try {
      Long startTs = request.getLockTs();
      Long currentTs = request.getCurrentTs();
      logger.info("kvCheckTxnStatus for txn: " + startTs);
      Kvrpcpb.CheckTxnStatusResponse.Builder builder = Kvrpcpb.CheckTxnStatusResponse.newBuilder();

      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      Supplier<Kvrpcpb.CheckTxnStatusResponse.Builder> txnStatus = txnStatusMap.get(startTs);
      if (txnStatus != null) {
        Kvrpcpb.CheckTxnStatusResponse resp = txnStatus.get().build();
        if (resp.getCommitVersion() == 0
            && resp.getLockTtl() > 0
            && TiTimestamp.extractPhysical(startTs) + resp.getLockInfo().getLockTtl()
                < TiTimestamp.extractPhysical(currentTs)) {
          ByteString key = resp.getLockInfo().getKey();
          logger.info(
              String.format(
                  "kvCheckTxnStatus rollback expired txn: %d, remove lock: %s",
                  startTs, key.toStringUtf8()));
          removeLock(key);
          putTxnStatus(startTs, 0L, ByteString.EMPTY);
          resp = txnStatusMap.get(startTs).get().build();
        }
        logger.info("kvCheckTxnStatus resp: " + resp);
        responseObserver.onNext(resp);
      } else {
        builder.setError(
            Kvrpcpb.KeyError.newBuilder()
                .setTxnNotFound(
                    Kvrpcpb.TxnNotFound.newBuilder()
                        .setPrimaryKey(request.getPrimaryKey())
                        .setStartTs(startTs)));
        logger.info("kvCheckTxnStatus, TxnNotFound");
        responseObserver.onNext(builder.build());
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("kvCheckTxnStatus error: " + e);
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void kvResolveLock(
      org.tikv.kvproto.Kvrpcpb.ResolveLockRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Kvrpcpb.ResolveLockResponse> responseObserver) {
    logger.info("KVMockServer.kvResolveLock");
    try {
      Long startTs = request.getStartVersion();
      Long commitTs = request.getCommitVersion();
      logger.info(
          String.format(
              "kvResolveLock for txn: %d, commitTs: %d, keys: %d",
              startTs, commitTs, request.getKeysCount()));
      Kvrpcpb.ResolveLockResponse.Builder builder = Kvrpcpb.ResolveLockResponse.newBuilder();

      Error e = verifyContext(request.getContext());
      if (e != null) {
        responseObserver.onNext(builder.setRegionError(e).build());
        responseObserver.onCompleted();
        return;
      }

      if (request.getKeysCount() == 0) {
        lockMap.entrySet().removeIf(entry -> entry.getValue().get().getLockVersion() == startTs);
      } else {
        for (int i = 0; i < request.getKeysCount(); i++) {
          removeLock(request.getKeys(i));
        }
      }

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
