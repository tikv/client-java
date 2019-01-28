package org.tikv.txn;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.RegionException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.operation.iterator.ConcreteScanIterator;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TxnRegionStoreClient;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.txn.type.ClientRPCResult;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * KV client of transaction
 * APIs for GET/PUT/DELETE/SCAN
 */
public class TxnKVClient implements AutoCloseable{
    private final static Logger LOG = LoggerFactory.getLogger(TxnKVClient.class);

    private final TiSession session;
    private final RegionManager regionManager;
    private ReadOnlyPDClient pdClient;

    private TxnKVClient(String addresses) {
        this.session = TiSession.create(TiConfiguration.createRawDefault(addresses));
        this.regionManager = session.getRegionManager();
        this.pdClient = session.getPDClient();
    }

    public static TxnKVClient createClient(String addresses) {
        return new TxnKVClient(addresses);
    }

    public TiSession getSession() {
        return session;
    }

    public TiTimestamp getTimestamp() {
        BackOffer bo = ConcreteBackOffer.newTsoBackOff();
        TiTimestamp timestamp = new TiTimestamp(0, 0);
        try {
            while(true) {
                try {
                    timestamp = pdClient.getTimestamp(bo);
                    break;
                } catch (final TiKVException e) {//retry is exhausted
                    bo.doBackOff(BackOffFunction.BackOffFuncType.BoPDRPC, e);
                }
            }
        } catch (GrpcException e1) {
            LOG.error("Get tso from pd failed,", e1);
        }
        return timestamp;
    }

    /**
     * Begin a new transaction
     * @return
     */
    public ITransaction begin() {
        return new TikvTransaction(this);
    }

    public ITransaction begin(Function<ITransaction,Boolean> function) {
        return new TikvTransaction(this, function);
    }

    //add backoff logic when encountered region error,ErrBodyMissing, and other errors
    public ClientRPCResult prewrite(BackOffer backOffer, List<Kvrpcpb.Mutation> mutations, byte[] primary, long lockTTL, long startTs, long regionId) {
        ClientRPCResult result = new ClientRPCResult(true, false, null);
        //send request
        Pair<TiRegion,Metapb.Store> regionStore = regionManager.getRegionStorePairByRegionId(regionId);
        TxnRegionStoreClient client = TxnRegionStoreClient.create(regionStore.first, regionStore.second, session);
        try {
            client.prewrite(backOffer,  ByteString.copyFrom(primary), mutations, startTs, lockTTL);
        } catch (final TiKVException | StatusRuntimeException e) {
            result.setSuccess(false);
            result.setRetry(e instanceof RegionException);//mark retryable, region error, should retry prewrite again
            result.setError(e.getMessage());
        }
        return result;
    }

    /**
     * Commit request of 2pc,
     * add backoff logic when encountered region error, ErrBodyMissing, and other errors
     * @param backOffer
     * @param keys
     * @param startTs
     * @param commitTs
     * @param regionId
     * @return
     */
    public ClientRPCResult commit(BackOffer backOffer, byte[][] keys, long startTs, long commitTs, long regionId) {
        ClientRPCResult result = new ClientRPCResult(true, false, null);
        //send request
        Pair<TiRegion,Metapb.Store> regionStore = regionManager.getRegionStorePairByRegionId(regionId);
        TxnRegionStoreClient client = TxnRegionStoreClient.create(regionStore.first, regionStore.second, session);
        List<ByteString> byteList = Lists.newArrayList();
        for(byte[] key : keys) {
            byteList.add(ByteString.copyFrom(key));
        }
        try {
            client.commit(backOffer, byteList, startTs, commitTs);
        } catch (final TiKVException | StatusRuntimeException e) {
            result.setSuccess(false);
            result.setRetry(e instanceof RegionException);//mark retryable, region error, should retry prewrite again
            result.setError(e.getMessage());
        }
        return result;
    }

    /**
     * Cleanup request of 2pc
     * @param backOffer
     * @param key
     * @param startTs
     * @param regionId
     * @return
     */
    public boolean cleanup(BackOffer backOffer, byte[] key, long startTs, long regionId) {
        try {
            Pair<TiRegion,Metapb.Store> regionStore = regionManager.getRegionStorePairByRegionId(regionId);
            TxnRegionStoreClient client = TxnRegionStoreClient.create(regionStore.first, regionStore.second, session);
            //send rpc request to tikv server
            client.cleanup(backOffer, ByteString.copyFrom(key), startTs);
            return true;
        } catch (final TiKVException e) {
            LOG.error("Cleanup process error, retry end, key={}, startTs={}, regionId=%s", new String(key), startTs, regionId);
            return false;
        }
    }

    /**
     * Request for batch rollback on TiKV, retry operation should be deal with Caller
     * @param backOffer
     * @param keys
     * @param startTs
     * @param regionId
     * @return
     */
    public ClientRPCResult batchRollbackReq(BackOffer backOffer, byte[][] keys, long startTs, long regionId) {
        ClientRPCResult result = new ClientRPCResult(true, false, null);
        List<ByteString> byteList = Lists.newArrayList();
        for(byte[] key : keys) {
            byteList.add(ByteString.copyFrom(key));
        }
        try {
            Pair<TiRegion,Metapb.Store> regionStore = regionManager.getRegionStorePairByRegionId(regionId);
            TxnRegionStoreClient client = TxnRegionStoreClient.create(regionStore.first, regionStore.second, session);
            //send request
            client.batchRollback(backOffer, byteList, startTs);
        } catch (final Exception e) {
            result.setSuccess(false);
            result.setRetry(e instanceof RegionException);//mark retryable, region error, should retry prewrite again
            result.setError(e.getMessage());
        }
        return result;
    }

    /**
     * Get value of key from TiKV
     * @param key
     * @return
     */
    public byte[] get(byte[] key) {
        ByteString byteKey = ByteString.copyFrom(key);
        BackOffer bo = ConcreteBackOffer.newGetBackOff();
        long version = 0;
        ByteString value = null;
        try {
            Pair<TiRegion,Metapb.Store> region = regionManager.getRegionStorePairByKey(byteKey);
            TxnRegionStoreClient client = TxnRegionStoreClient.create(region.first, region.second, session);
            version =  getTimestamp().getVersion();
            value = client.get(bo, byteKey, version);
        } catch (final TiKVException | StatusRuntimeException e) {
            LOG.error("Get process error, key={}, version={}", new String(key), version);
        }

        return value != null ? value.toByteArray() : new byte[0];
    }

    /**
     * Put a new key-value pair to TiKV
     * @param key
     * @param value
     * @return
     */
    public boolean put(byte[] key, byte[] value) {
        boolean putResult = false;
        ByteString byteKey = ByteString.copyFrom(key);
        ByteString byteValue = ByteString.copyFrom(value);
        BackOffer bo = ConcreteBackOffer.newCustomBackOff(BackOffer.prewriteMaxBackoff);
        List<Kvrpcpb.Mutation> mutations = Lists.newArrayList(
                Kvrpcpb.Mutation.newBuilder()
                        .setKey(byteKey).setValue(byteValue).setOp(Kvrpcpb.Op.Put)
                        .build()
        );
        long lockTTL = 2000;
        long startTS;
        TiRegion region = regionManager.getRegionByKey(byteKey);
        boolean prewrite = false;
        while(true) {
            try {
                startTS = this.getTimestamp().getVersion();
                ClientRPCResult prewriteResp = this.prewrite(bo, mutations, key, lockTTL, startTS, region.getId());
                if(prewriteResp.isSuccess() || (!prewriteResp.isSuccess() && !prewriteResp.isRetry())) {
                    if(prewriteResp.isSuccess()) {
                        prewrite = true;
                    }
                    break;
                }
                LOG.error("Put process error, prewrite try next time, error={}", prewriteResp.getError());
                bo.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, new TiKVException(prewriteResp.getError()));
            } catch (final TiKVException e) {
                LOG.error("Put process error, 2pc prewrite failed,", e);
            }
        }
        if(prewrite) {
            long commitTs;
            byte[][] keys = new byte[1][];
            keys[0] = key;
            while(true) {
                try {
                    commitTs = this.getTimestamp().getVersion();
                    region = regionManager.getRegionByKey(byteKey);
                    ClientRPCResult commitResp = this.commit(bo, keys, startTS, commitTs, region.getId());
                    if(commitResp.isSuccess() || (!commitResp.isSuccess() && !commitResp.isRetry())) {
                        if(commitResp.isSuccess()) {
                            putResult = true;
                        }
                        break;
                    }
                    LOG.error("Put process failed, commit try next time, error={}", commitResp.getError());
                    bo.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, new TiKVException(commitResp.getError()));
                } catch (final TiKVException e) {
                    LOG.error("Put process error, 2pc commit failed,", e);
                }
            }
        }
        return putResult;
    }

    /**
     * Delete a key value from TiKV
     * @param key the key will be deleted
     */
    public boolean delete(byte[] key) {
        boolean putResult = false;
        ByteString byteKey = ByteString.copyFrom(key);
        BackOffer bo = ConcreteBackOffer.newCustomBackOff(BackOffer.prewriteMaxBackoff);
        List<Kvrpcpb.Mutation> mutations = Lists.newArrayList(
                Kvrpcpb.Mutation.newBuilder()
                        .setKey(byteKey).setOp(Kvrpcpb.Op.Del)
                        .build()
        );
        long lockTTL = 2000;
        long startTS;
        TiRegion region = regionManager.getRegionByKey(byteKey);
        boolean prewrite = false;
        while(true) {
            try {
                startTS = this.getTimestamp().getVersion();
                ClientRPCResult prewriteResp = this.prewrite(bo, mutations, key, lockTTL, startTS, region.getId());
                if(prewriteResp.isSuccess() || (!prewriteResp.isSuccess() && !prewriteResp.isRetry())) {
                    if(prewriteResp.isSuccess()) {
                        prewrite = true;
                    }
                    break;
                }
                LOG.error("Delete process error, prewrite try next time, error={}", prewriteResp.getError());
                bo.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, new TiKVException(prewriteResp.getError()));
            } catch (final TiKVException e) {
                LOG.error("Delete process error, 2pc prewrite failed,", e);
            }
        }
        if(prewrite) {
            long commitTs;
            byte[][] keys = new byte[1][];
            keys[0] = key;
            while(true) {
                try {
                    commitTs = this.getTimestamp().getVersion();
                    region = regionManager.getRegionByKey(byteKey);
                    ClientRPCResult commitResp = this.commit(bo, keys, startTS, commitTs, region.getId());
                    if(commitResp.isSuccess() || (!commitResp.isSuccess() && !commitResp.isRetry())) {
                        if(commitResp.isSuccess()) {
                            putResult = true;
                        }
                        break;
                    }
                    LOG.error("Delete process failed, commit try next time, error={}", commitResp.getError());
                    bo.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, new TiKVException(commitResp.getError()));
                } catch (final TiKVException e) {
                    LOG.error("Put process error, 2pc commit failed,", e);
                }
            }
        }
        return putResult;
    }

    /**
     * Scan key-value pair from TiKV
     * @param startKey start key
     * @param limit max limit count
     * @return
     */
    public List<Pair<byte[], byte[]>> scan(byte[] startKey, int limit) {
        ByteString byteKey = ByteString.copyFrom(startKey);
        long version = getTimestamp().getVersion();
        ConcreteScanIterator iterator = new ConcreteScanIterator(byteKey, session, version);
        List<Pair<byte[], byte[]>> result = new LinkedList<>();
        int count = 0;
        while(iterator.hasNext() && count ++ < limit) {
            Kvrpcpb.KvPair pair = iterator.next();
            result.add(Pair.create(pair.getKey().toByteArray(), pair.getValue().toByteArray()));
        }
        return result;
    }

    private BackOffer defaultBackOff() {
        return ConcreteBackOffer.newCustomBackOff(1000);
    }

    @Override
    public void close() throws Exception {
        session.close();
    }
}