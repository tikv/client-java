package org.tikv.common.region;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.*;
import org.tikv.common.streaming.BatchStreamConnection;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.Tikvpb;

public class RegionStoreAsyncClient {
  private final TiConfiguration conf;
  private final BatchStreamConnection connection;
  private final Metapb.Store store;
  private final TiRegion region;

  public RegionStoreAsyncClient(
      TiConfiguration conf, BatchStreamConnection connection, Metapb.Store store, TiRegion region) {
    this.conf = conf;
    this.connection = connection;
    this.store = store;
    this.region = region;
  }

  public ByteString rawGet(ByteString key) throws TiKVException {
    Kvrpcpb.RawGetRequest request =
        Kvrpcpb.RawGetRequest.newBuilder().setContext(region.getContext()).setKey(key).build();
    Tikvpb.BatchCommandsRequest.Request.Builder builder =
        Tikvpb.BatchCommandsRequest.Request.newBuilder();
    builder.setRawGet(request);
    Future<Tikvpb.BatchCommandsResponse.Response> f =
        this.connection.async_request(builder.build());
    try {
      Tikvpb.BatchCommandsResponse.Response response = f.get();
      return rawGetHelper(response.getRawGet());
    } catch (InterruptedException e) {
      throw new GrpcException("this request has been interrupted because the thread has stopped");
    } catch (ExecutionException e) {
      throw new GrpcException(
          "this request has been interrupted because the connection has aborted");
    } catch (TiKVException e) {
      throw e;
    }
  }

  private ByteString rawGetHelper(Kvrpcpb.RawGetResponse resp) {
    if (resp == null) {
      throw new TiClientInternalException("RawGetResponse failed without a cause");
    }
    String error = resp.getError();
    if (!error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getValue();
  }

  public static class RegionStoreAsyncClientBuilder {
    private final TiConfiguration conf;
    private final ChannelFactory channelFactory;
    private final Map<Long, BatchStreamConnection> connectionMap;
    private final List<Thread> threads;
    private final RegionManager regionManager;
    private final PDClient pdClient;

    public RegionStoreAsyncClientBuilder(
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
      this.connectionMap = new HashMap<>();
      this.threads = new LinkedList<>();
    }

    public RegionStoreAsyncClient build(ByteString key) throws GrpcException {
      return build(key, TiStoreType.TiKV);
    }

    public RegionStoreAsyncClient build(ByteString key, TiStoreType storeType)
        throws GrpcException {
      Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByKey(key, storeType);
      return build(pair.first, pair.second, storeType);
    }

    public RegionStoreAsyncClient build(TiRegion region, Metapb.Store store, TiStoreType storeType)
        throws GrpcException {
      Objects.requireNonNull(region, "region is null");
      Objects.requireNonNull(store, "store is null");
      Objects.requireNonNull(storeType, "storeType is null");

      BatchStreamConnection connection = getOrCreateConnection(store);

      return new RegionStoreAsyncClient(conf, connection, store, region);
    }

    public synchronized BatchStreamConnection getOrCreateConnection(Metapb.Store store)
        throws GrpcException {
      Long storeId = store.getId();
      BatchStreamConnection connection = connectionMap.get(storeId);
      if (connection != null) {
        return connection;
      }
      String addressStr = store.getAddress();
      ManagedChannel channel = channelFactory.getChannel(addressStr);

      TikvGrpc.TikvStub asyncStub = TikvGrpc.newStub(channel);
      connection = new BatchStreamConnection(asyncStub, store);
      BatchStreamConnection finalConnection = connection;
      this.threads.add(
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  finalConnection.run();
                }
              }));
      return connection;
    }
  }
}
