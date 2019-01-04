package org.tikv.common.region;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import org.apache.log4j.Logger;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.TiSession;
import org.tikv.common.util.BackOffer;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.txn.LockResolverClient;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * RegionStore itself is not thread-safe
 * APIs for Transaction KV Scan/Put/Get/Delete
 * @author jackycchen
 */
public class TxnRegionStoreClient extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvStub>
        implements RegionErrorReceiver {
    private static final Logger logger = Logger.getLogger(TxnRegionStoreClient.class);

    private TiRegion region;
    private final RegionManager regionManager;
    @VisibleForTesting
    private final LockResolverClient lockResolverClient;
    private TikvGrpc.TikvBlockingStub blockingStub;
    private TikvGrpc.TikvStub asyncStub;

    private TxnRegionStoreClient(
            TiRegion region, TiSession session, TikvGrpc.TikvBlockingStub blockingStub, TikvGrpc.TikvStub asyncStub) {
        super(session);
        checkNotNull(region, "Region is empty");
        checkNotNull(region.getLeader(), "Leader Peer is null");
        checkArgument(region.getLeader() != null, "Leader Peer is null");
        this.regionManager = session.getRegionManager();
        this.region = region;
        this.blockingStub = blockingStub;
        this.asyncStub = asyncStub;
        this.lockResolverClient = new LockResolverClient(session, this.blockingStub, this.asyncStub);
    }

    public static TxnRegionStoreClient create(TiRegion region, Metapb.Store store, TiSession session) {
        TxnRegionStoreClient client;
        String addressStr = store.getAddress();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Create region store client on address %s", addressStr));
        }
        ManagedChannel channel = session.getChannel(addressStr);

        TikvGrpc.TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);

        TikvGrpc.TikvStub asyncStub = TikvGrpc.newStub(channel);
        client = new TxnRegionStoreClient(region, session, blockingStub, asyncStub);
        return client;
    }

    @Override
    protected TikvGrpc.TikvBlockingStub getBlockingStub() {
        return blockingStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
    }

    @Override
    protected TikvGrpc.TikvStub getAsyncStub() {
        return asyncStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public boolean onNotLeader(Metapb.Store newStore) {
        if (logger.isDebugEnabled()) {
            logger.debug(region + ", new leader = " + newStore.getId());
        }
        TiRegion cachedRegion = regionManager.getRegionById(region.getId());
        // When switch leader fails or the region changed its key range,
        // it would be necessary to re-split task's key range for new region.
        if (!region.getStartKey().equals(cachedRegion.getStartKey())
                || !region.getEndKey().equals(cachedRegion.getEndKey())) {
            return false;
        }
        region = cachedRegion;
        String addressStr = regionManager.getStoreById(region.getLeader().getStoreId()).getAddress();
        ManagedChannel channel = getSession().getChannel(addressStr);
        blockingStub = TikvGrpc.newBlockingStub(channel);
        asyncStub = TikvGrpc.newStub(channel);
        return true;
    }

    @Override
    public void onStoreNotMatch(Metapb.Store store) {
        String addressStr = store.getAddress();
        ManagedChannel channel = getSession().getChannel(addressStr);
        blockingStub = TikvGrpc.newBlockingStub(channel);
        asyncStub = TikvGrpc.newStub(channel);
        if (logger.isDebugEnabled() && region.getLeader().getStoreId() != store.getId()) {
            logger.debug(
                    "store_not_match may occur? "
                            + region
                            + ", original store = "
                            + store.getId()
                            + " address = "
                            + addressStr);
        }
    }

    // APIs for Transaction KV Scan/Put/Get/Delete
    public ByteString get(BackOffer backOffer, ByteString key, long version) {

        return null;
    }

    // TODO: batch get should consider key range split
    public List<Kvrpcpb.KvPair> batchGet(BackOffer backOffer, Iterable<ByteString> keys, long version) {

        return null;
    }

    public List<Kvrpcpb.KvPair> scan(BackOffer backOffer, ByteString startKey, long version) {
        return scan(backOffer, startKey, version, false);
    }

    public List<Kvrpcpb.KvPair> scan(
            BackOffer backOffer, ByteString startKey, long version, boolean keyOnly) {

        return null;
    }

    public void put(BackOffer backOffer, ByteString key, ByteString value) {


    }

    public ByteString get(BackOffer backOffer, ByteString key) {

        return  null;
    }

    public void delete(BackOffer backOffer, ByteString key) {

    }
}
