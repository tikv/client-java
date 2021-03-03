package org.tikv.common.streaming;

import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.tikv.common.exception.GrpcException;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.Tikvpb;

public class BatchStreamConnection {
  private static final int MAX_WAIT_RESPONSE_FOR_SLEEP = 64;
  private static final int MAX_BATCH_SIZE = 128;
  private static final long CLOSE_ID = 0L;

  private final TikvGrpc.TikvStub stub;
  private final Metapb.Store store;
  private final StreamObserver<Tikvpb.BatchCommandsRequest> requestsObserver;
  private final StreamObserver<Tikvpb.BatchCommandsResponse> responseObserver;
  private final AtomicLong globalId;
  private final Map<Long, CompletableFuture<Tikvpb.BatchCommandsResponse.Response>>
      responseCallables;
  private final BlockingQueue<RequestTask> queue;
  private final AtomicBoolean running;

  public BatchStreamConnection(TikvGrpc.TikvStub stub, Metapb.Store store) {
    this.stub = stub;
    this.store = store;
    this.globalId = new AtomicLong(CLOSE_ID + 1);
    this.responseCallables = new HashMap<>();
    this.queue = new ArrayBlockingQueue<RequestTask>(1024);
    this.running = new AtomicBoolean(false);
    this.responseObserver =
        new StreamObserver<Tikvpb.BatchCommandsResponse>() {
          @Override
          public void onNext(Tikvpb.BatchCommandsResponse batchCommandsResponse) {
            List<Long> ids = batchCommandsResponse.getRequestIdsList();
            List<Tikvpb.BatchCommandsResponse.Response> responses =
                batchCommandsResponse.getResponsesList();
            for (int i = 0; i < ids.size(); i++) {
              Tikvpb.BatchCommandsResponse.Response response = responses.get(i);
              Long id = ids.get(i);
              CompletableFuture<Tikvpb.BatchCommandsResponse.Response> callable =
                  responseCallables.get(id);
              callable.complete(response);
            }
          }

          @Override
          public void onError(Throwable throwable) {
            running.set(false);
            synchronized (responseCallables) {
              for (Map.Entry<Long, CompletableFuture<Tikvpb.BatchCommandsResponse.Response>> entry :
                  responseCallables.entrySet()) {
                entry.getValue().completeExceptionally(throwable);
              }
            }
          }

          @Override
          public void onCompleted() {
            running.set(false);
            cancelRequest();
          }
        };
    this.requestsObserver = this.stub.batchCommands(this.responseObserver);
  }

  private static class RequestTask {
    private final Tikvpb.BatchCommandsRequest.Request request;
    private final CompletableFuture<Tikvpb.BatchCommandsResponse.Response> callable;
    private final Long id;

    private RequestTask(
        Tikvpb.BatchCommandsRequest.Request request,
        CompletableFuture<Tikvpb.BatchCommandsResponse.Response> callable,
        Long id) {
      this.request = request;
      this.callable = callable;
      this.id = id;
    }
  }

  public Future<Tikvpb.BatchCommandsResponse.Response> async_request(
      Tikvpb.BatchCommandsRequest.Request request) {
    CompletableFuture<Tikvpb.BatchCommandsResponse.Response> future = new CompletableFuture<>();
    Long id = globalId.addAndGet(1);
    RequestTask task = new RequestTask(request, future, id);
    return future;
  }

  public void close() {
    this.running.set(false);
    this.requestsObserver.onCompleted();
    Tikvpb.BatchCommandsRequest.Request request =
        Tikvpb.BatchCommandsRequest.Request.newBuilder().build();
    this.queue.offer(new RequestTask(request, new CompletableFuture<>(), CLOSE_ID));
    cancelRequest();
  }

  private void cancelRequest() {
    synchronized (responseCallables) {
      for (Map.Entry<Long, CompletableFuture<Tikvpb.BatchCommandsResponse.Response>> entry :
          responseCallables.entrySet()) {
        entry
            .getValue()
            .completeExceptionally(
                new GrpcException(
                    "send tikv request error, because connection to ["
                        + store.getAddress()
                        + "] has closed, try another store later"));
      }
    }
  }

  public void run() {
    running.set(true);
    while (running.get()) {
      Tikvpb.BatchCommandsRequest.Builder builder = Tikvpb.BatchCommandsRequest.newBuilder();
      try {
        RequestTask request = queue.take();
        if (request.id == CLOSE_ID) {
          break;
        }
        int waitingResponseCount = addRequest(request, builder);
        if (waitingResponseCount > MAX_WAIT_RESPONSE_FOR_SLEEP) {
          Thread.sleep(1L);
        }
      } catch (Exception e) {
        break;
      }
      while (!queue.isEmpty()) {
        try {
          RequestTask request = queue.take();
          if (request.id == CLOSE_ID) {
            this.running.set(false);
            cancelRequest();
            return;
          }
          addRequest(request, builder);
          if (builder.getRequestIdsCount() > MAX_BATCH_SIZE) {
            break;
          }
        } catch (Exception e) {
          break;
        }
      }
      Tikvpb.BatchCommandsRequest request = builder.build();
      requestsObserver.onNext(request);
    }
    cancelRequest();
  }

  private int addRequest(RequestTask task, Tikvpb.BatchCommandsRequest.Builder builder) {
    int waitingResponseCount = 0;
    synchronized (this.responseCallables) {
      this.responseCallables.put(task.id, task.callable);
      waitingResponseCount = this.responseCallables.size();
    }
    builder.addRequests(task.request);
    builder.addRequestIds(task.id);
    return waitingResponseCount;
  }
}
