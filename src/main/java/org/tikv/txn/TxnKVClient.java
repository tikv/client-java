package org.tikv.txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.region.RegionManager;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.txn.pool.SecondaryCommitTaskThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TxnKVClient {
    private final static Logger LOG = LoggerFactory.getLogger(TxnKVClient.class);

    // TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
    // Key+Value size below 16KB.
    private static final int txnCommitBatchSize = 16 * 1024;
    private static final SecondaryCommitTaskThreadPool secondaryCommitPool = new SecondaryCommitTaskThreadPool();

    private Map<String, Kvrpcpb.Mutation> mutations = new HashMap<>();
    private List<byte[]> keysList;

    private final TiSession session;
    private final RegionManager regionManager;
    private ReadOnlyPDClient pdClient;

    private volatile boolean prewriteTaskError = false;
    private volatile AtomicInteger seondaryThreadIdGenerator = new AtomicInteger(0);

    private TxnKVClient(String addresses) {
        this.session = TiSession.create(TiConfiguration.createRawDefault(addresses));
        this.regionManager = session.getRegionManager();
        this.pdClient = session.getPDClient();
    }

    public static TxnKVClient createTxnClient(String addresses) {
        return new TxnKVClient(addresses);
    }

    public TiSession getSession() {
        return session;
    }
}
