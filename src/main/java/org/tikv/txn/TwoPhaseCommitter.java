package org.tikv.txn;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.txn.pool.SecondaryCommitTaskThreadPool;
import org.tikv.txn.type.BatchKeys;
import org.tikv.txn.type.ClientRPCResult;
import org.tikv.txn.type.GroupKeyResult;
import org.tikv.txn.type.TwoPhaseCommitType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 2PC implementation of TiKV
 */
public class TwoPhaseCommitter {
    private final static Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitter.class);

    // TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
    // Key+Value size below 16KB.
    private static final int txnCommitBatchSize = 16 * 1024;
    private static final long defaultLockTTL = 3000;//unit is second
    private static final int bytesPerMiB = 1024 * 1024;
    private long maxTxnTimeUse = 60_1000;//unit is: milliseconds
    // ttl = ttlFactor * sqrt(writeSizeInMiB)
    private static final int ttlFactor = 6000;
    private static final int maxLockTTL = 12000;
    private static final SecondaryCommitTaskThreadPool secondaryCommitPool = new SecondaryCommitTaskThreadPool();

    private Map<String, Kvrpcpb.Mutation> mutations = new LinkedHashMap<>();
    private List<byte[]> keysList;
    private ReadOnlyPDClient pdClient;
    private TxnKVClient kvClient;

    private long lockTTL = 0;
    /**
     * start timestamp of transaction which get from PD
     */
    private long startTs = 0;
    /**
     * commit timestamp of transaction which get from PD
     */
    private long commitTs = 0;

    private volatile boolean prewriteTaskError = false;
    //private volatile AtomicInteger seondaryThreadIdGenerator = new AtomicInteger(0);

    private TwoPhaseCommitter() {}

    public TwoPhaseCommitter(ITransaction transaction) {
        this.pdClient = transaction.getKVClient().getSession().getPDClient();
        this.keysList = new LinkedList<>();
        this.kvClient = transaction.getKVClient();
        this.startTs = transaction.getStartTS();

        Map<byte[], byte[]> storedKeys = transaction.getStoredKeys();
        int putCount = 0, delCount = 0, lockCount = 0;
        int txnSize = 0;
        for(byte[] key : storedKeys.keySet()) {
            byte[] value = storedKeys.get(key);
            if(value.length > 0) {
                Kvrpcpb.Mutation mutation = Kvrpcpb.Mutation.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .setOp(Kvrpcpb.Op.Put)
                        .build();
                mutations.put(new String(key), mutation);
                putCount++;
            } else {
                Kvrpcpb.Mutation mutation = Kvrpcpb.Mutation.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setOp(Kvrpcpb.Op.Del)
                        .build();
                mutations.put(new String(key), mutation);
                delCount ++;
            }
            keysList.add(key);
            txnSize += (key.length + value.length);
            //TODO check transaction maxEntrySize
        }
        List<byte[]> lockedKeys = transaction.getLockedKeys();
        for(byte[] lockedKey : lockedKeys) {
            Kvrpcpb.Mutation mutation = Kvrpcpb.Mutation.newBuilder()
                    .setKey(ByteString.copyFrom(lockedKey))
                    .setOp(Kvrpcpb.Op.Lock)
                    .build();
            mutations.put(new String(lockedKey), mutation);
            lockCount ++;
            keysList.add(lockedKey);
            txnSize += (lockedKey.length);
        }
        this.lockTTL = getTxnLockTTL(transaction.getStartTime(), txnSize);
        LOG.debug("Txn info, startTs={}, putCount={}, delCount={}, lockCount={}, lockTTL={}", startTs, putCount, delCount, lockCount, lockTTL);
    }

    private long getTxnLockTTL(long startTime, int txnSize) {
        // Increase lockTTL for large transactions.
        // The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
        // When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
        // When writeSize is 1MiB, 100MiB, or 400MiB, ttl is 6s, 60s, 120s correspondingly;
        long ttl = defaultLockTTL;
        if(txnSize >= txnCommitBatchSize) {
            int sizeInMiB = txnSize / bytesPerMiB;
            ttl = (long)(ttlFactor* Math.sqrt(sizeInMiB));
            if(ttl < defaultLockTTL) {
                ttl = defaultLockTTL;
            }
            if(ttl > maxLockTTL) {
                ttl = maxLockTTL;
            }
        }
        // Increase lockTTL by the transaction's read time.
        // When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
        // takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
        long elapsed = (System.currentTimeMillis() - startTime);
        if(elapsed < 0) {//should not happened forever
            LOG.warn("transaction startTime elapsed invalid, startTime={}, elapsed={}", startTime, elapsed);
        }
        return ttl + elapsed;
    }

    private GroupKeyResult groupKeysByRegion(BackOffer backOffer, byte[][] keys) {
        Map<Long, List<byte[]>> groups = new HashMap<>();
        long first = 0;
        int index = 0;
        String error = null;
        try {
            for(; index < keys.length; index ++) {
                byte[] key = keys[index];
                TiRegion tiRegion = this.pdClient.getRegionByKey(backOffer, ByteString.copyFrom(key));
                if(tiRegion != null){
                    Long regionId = tiRegion.getId();
                    if(index == 0) {
                        first = regionId;
                    }
                    List<byte[]> groupItem = groups.computeIfAbsent(regionId, e -> new LinkedList<>());
                    /*List<byte[]> groupItem = groups.get(regionId);
                    if(groupItem != null) {
                        groupItem = new LinkedList<>();
                    }*/
                    groupItem.add(key);
                    groups.put(tiRegion.getId(), groupItem);
                }
            }
        } catch (Exception e) {
            error = String.format("Txn groupKeysByRegion error, %s", e.getMessage());
        }
        GroupKeyResult result = new GroupKeyResult();
        if(error == null) {
            result.setFirstRegion(first);
            result.setGroupsResult(groups);
        }
        result.setErrorMsg(error);
        return result;
    }

    private void appendBatchBySize(List<BatchKeys> batchKeyList, Long regionId, List<byte[]> keys, boolean sizeKeyValue, int limit) {
        int start, end = 0;
        int len = keys.size();
        for(start = 0; start < len; start = end) {
            int size = 0;
            for(end = start; end < len && size < limit; end ++) {
                if(sizeKeyValue) {
                    size += this.keyValueSize(keys.get(end));
                } else {
                    size += this.keySize(keys.get(end));
                }
            }
            BatchKeys batchKeys = new BatchKeys(regionId, keys.subList(start, end));
            batchKeyList.add(batchKeys);
        }
    }

    private boolean isPrimaryKey(byte[] key) {
        return this.keysList != null && FastByteComparisons.compareTo(this.keysList.get(0), key) == 0;
    }

    protected String doActionOnKeys(BackOffer backOffer, TwoPhaseCommitType actionType, byte[][] keys) {
        if(keys == null || keys.length == 0) {
            return null;
        }
        //split to groups
        GroupKeyResult groupResult = this.groupKeysByRegion(backOffer, keys);
        if(groupResult.hasError()) {
            return groupResult.getErrorMsg();
        }
        boolean sizeKeyValue = false;
        if(actionType == TwoPhaseCommitType.actionPrewrite) {
            sizeKeyValue = true;
        }
        List<BatchKeys> batchKeyList = new LinkedList<>();
        Map<Long, List<byte[]>>  groupKeyMap = groupResult.getGroupsResult();
        long firstRegion = groupResult.getFirstRegion();
        // Make sure the group that contains primary key goes first.
        this.appendBatchBySize(batchKeyList, firstRegion, groupKeyMap.get(firstRegion),
                sizeKeyValue, txnCommitBatchSize);
        groupKeyMap.remove(firstRegion);
        for(Long regionId : groupKeyMap.keySet()) {
            this.appendBatchBySize(batchKeyList, regionId, groupKeyMap.get(regionId),
                    sizeKeyValue, txnCommitBatchSize);
        }
        boolean firstIsPrimary =  this.isPrimaryKey(keys[0]);

        if (firstIsPrimary && (actionType == TwoPhaseCommitType.actionCommit
                || actionType == TwoPhaseCommitType.actionCleanup)) {
            // primary should be committed/cleanup first
            String error = this.doActionOnBatches(backOffer, actionType, batchKeyList.subList(0, 1));
            if(error != null) {
                return error;
            }
            batchKeyList.remove(0);
        }
        String error = null;
        if(actionType == TwoPhaseCommitType.actionCommit) {
            // Commit secondary batches in background goroutine to reduce latency.
            secondaryCommitPool.submitSecondaryTask(() -> {
                if(prewriteTaskError) {
                    LOG.error("Txn 2PC async doActionOnBatches canceled, other secondary thread failed");
                    return;
                }
                String errorInner = doActionOnBatches(backOffer, actionType, batchKeyList);
                if(errorInner != null) {
                    LOG.warn("Txn 2PC async doActionOnBatches error: {}", errorInner);
                    prewriteTaskError = true;
                }
            });
        } else {
            error = this.doActionOnBatches(backOffer, actionType, batchKeyList);
        }
        return error;
    }

    //doActionOnBatches does action to batches in parallel.
    public String doActionOnBatches(BackOffer backOffer, TwoPhaseCommitType actionType, List<BatchKeys> batchKeys) {
        if(batchKeys.size() == 0) {
            LOG.debug("Txn 2PC doActionOnBatches batch keys is empty: type={}", actionType);
            return null;
        }
        switch(actionType) {
            case actionPrewrite: {
                return this.doPrewriteActionOnBatches(backOffer, batchKeys);
            }
            case actionCommit: {
                return this.doCommitActionOnBatches(backOffer, batchKeys);
            }
            case actionCleanup: {
                return this.doCleanupActionOnBatches(backOffer, batchKeys);
            }
        }
        return null;
    }

    private String doPrewriteActionOnBatches(BackOffer backOffer, List<BatchKeys> batchKeysList) {
        if(batchKeysList.size() == 1) {
            return this.prewriteSingleBatch(backOffer, batchKeysList.get(0));
        }
        // For prewrite, stop sending other requests after receiving first error.
        for(BatchKeys batchKeys : batchKeysList) {
            String error = prewriteSingleBatch(backOffer, batchKeys);
            if(error != null) {
                //single to other thread error happened
                prewriteTaskError = true;
                //LOG.warn("Txn 2PC doPrewriteActionOnBatches failed, send to other request: {}", error);
                return error;
            }
        }
        return null;
    }

    private String doCommitActionOnBatches(BackOffer backOffer, List<BatchKeys> batchKeysList) {
        if(batchKeysList.size() == 1) {
            return this.commitSingleBatch(backOffer, batchKeysList.get(0));
        }
        // For prewrite, stop sending other requests after receiving first error.
        for(BatchKeys batchKeys : batchKeysList) {
            String error = commitSingleBatch(backOffer, batchKeys);
            if(error != null) {
                //LOG.warn("Txn 2PC doCommitActionOnBatches failed, send to other request: {}", error);
                return error;
            }
        }
        return null;
    }

    private String doCleanupActionOnBatches(BackOffer backOffer, List<BatchKeys> batchKeysList) {
        if(batchKeysList.size() == 1) {
            return this.prewriteSingleBatch(backOffer, batchKeysList.get(0));
            /*if(error != null) {
                LOG.error("Txn 2PC doCleanupActionOnBatches failed, one batch size, error: {}", error);
            }
            return error;*/
        }
        // For prewrite, stop sending other requests after receiving first error.
        for(BatchKeys batchKeys : batchKeysList) {
            String error = cleanupSingleBatch(backOffer, batchKeys);
            if(error != null) {
                //single to other thread error happened
                prewriteTaskError = true;
                LOG.warn("Txn 2PC doCleanupActionOnBatches failed, send to other request: {}", error);
                return error;
            }
        }
        return null;
    }

    public long keyValueSize(byte[] key) {
        long size = key.length;
        String keyStr = new String(key);
        Kvrpcpb.Mutation mutation = this.mutations.get(keyStr);
        if(mutation != null) {
            size += mutation.getValue().toByteArray().length;
        }

        return size;
    }

    public long keySize(byte[] key) {
        return key.length;
    }

    private byte[] primaryKey() {
        return this.keysList.get(0);
    }

    public String prewriteSingleBatch(BackOffer backOffer, BatchKeys batchKeys) {
        List<byte[]> keyList = batchKeys.getKeys();
        int batchSize = keyList.size();
        byte[][] keys = new byte[batchSize][];
        int index = 0;
        List<Kvrpcpb.Mutation> mutationList = new ArrayList<>(batchSize);
        for(byte[] key : keyList) {
            mutationList.add(mutations.get(new String(key)));
            keys[index++] = key;
        }
        //send rpc request to tikv server
        long regionId = batchKeys.getRegioId();
        ClientRPCResult prewriteResult = this.kvClient.prewriteReq(backOffer, mutationList, primaryKey(),
                this.lockTTL, this.startTs, regionId);
        if(!prewriteResult.isSuccess() && !prewriteResult.isRetry()) {
            return String.format("Txn prewriteSingleBatch error, regionId=%s", batchKeys.getKeys());
        }
        if(!prewriteResult.isSuccess() && prewriteResult.isRetry()) {
            try {
                backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss,
                        new GrpcException(String.format("Txn prewriteSingleBatch failed, regionId=%s, detail=%s", batchKeys.getRegioId(), prewriteResult.getError())));
                // re-split keys and commit again.
                String error = this.prewriteKeys(backOffer, keys);
                return error;
            } catch (GrpcException e) {
                String error = String.format("Txn prewriteSingleBatch error, re-split commit failed, regionId=%s, detail=%s", batchKeys.getRegioId(), e.getMessage());
                LOG.error(error);
                return error;
            }
        }
        //success return
        return null;
    }

    public String commitSingleBatch(BackOffer backOffer, BatchKeys batchKeys) {
        List<byte[]> keysCommit = batchKeys.getKeys();
        byte[][] keys = new byte[keysCommit.size()][];
        keysCommit.toArray(keys);
        //send rpc request to tikv server
        long regionId = batchKeys.getRegioId();
        ClientRPCResult commitResult = this.kvClient.commitReq(backOffer, keys,
                this.startTs, this.commitTs, regionId);
        if(!commitResult.isSuccess() && !commitResult.isRetry()) {
            String error = String.format("Txn commitSingleBatch error, regionId=%s", batchKeys.getRegioId());
            LOG.error(error);
            return error;
        }
        if(!commitResult.isSuccess() && commitResult.isRetry()) {
            try {
                backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss,
                        new GrpcException(String.format("Txn commitSingleBatch failed, regionId=%s", batchKeys.getRegioId())));
                // re-split keys and commit again.
                String error = this.commitKeys(backOffer, keys);
                if(error != null) {
                    LOG.error(error);
                    return error;
                }
            } catch (GrpcException e) {
                String error = String.format("Txn commitSingleBatch error, re-split commit failed, regionId=%s", batchKeys.getKeys());
                LOG.error(error);
                return error;
            }
        }
        if(!commitResult.isSuccess()) {
            String error = String.format("Txn commitSingleBatch error, regionId=%s, detail=%s", batchKeys.getKeys(), commitResult.getError());
            LOG.error(error);
            return error;
        }
        return null;
    }

    public String cleanupSingleBatch(BackOffer backOffer, BatchKeys batchKeys) {
        byte[][] keys = new byte[batchKeys.getKeys().size()][];
        batchKeys.getKeys().toArray(keys);
        ClientRPCResult rollbackResult = this.kvClient.batchRollbackReq(backOffer, keys, this.startTs, batchKeys.getRegioId());
        if(!rollbackResult.isSuccess() && !rollbackResult.isRetry()) {
            String error = String.format("Txn cleanupSingleBatch error, regionId=%s", batchKeys.getKeys());
            LOG.error(error);
            return error;
        }
        if(!rollbackResult.isSuccess() && rollbackResult.isRetry()) {
            try {
                backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss,
                        new GrpcException(String.format("Txn cleanupSingleBatch failed, regionId=%s", batchKeys.getRegioId())));
                // re-split keys and commit again.
                String error = this.cleanupKeys(backOffer, keys);
                if(error != null) {
                    LOG.error(error);
                    return error;
                }
            } catch (GrpcException e) {
                String error = String.format("Txn cleanupSingleBatch error, re-split commit failed, regionId=%s", batchKeys.getKeys());
                LOG.error(error);
                return error;
            }
        }
        if(!rollbackResult.isSuccess()) {
            String error = String.format("Txn cleanupSingleBatch error, regionId=%s, detail=%s", batchKeys.getKeys(), rollbackResult.getError());
            LOG.error(error);
            return error;
        }
        return null;
    }

    /**
     * 2pc - prewrite phase
     * @param backOffer
     * @param keys
     * @return
     */
    public String prewriteKeys(BackOffer backOffer, byte[][] keys) {

        return this.doActionOnKeys(backOffer, TwoPhaseCommitType.actionPrewrite, keys);
    }

    /**
     * 2pc - commit phase
     * @param backOffer
     * @param keys
     * @return
     */
    public String commitKeys(BackOffer backOffer, byte[][] keys) {

        return this.doActionOnKeys(backOffer, TwoPhaseCommitType.actionCommit, keys);
    }

    /**
     * 2pc - cleanup phase
     * @param backOffer
     * @param keys
     * @return
     */
    public String cleanupKeys(BackOffer backOffer, byte[][] keys) {

        return this.doActionOnKeys(backOffer, TwoPhaseCommitType.actionCleanup, keys);
    }

    public boolean execute() {
        BackOffer prewriteBackoff =ConcreteBackOffer.newCustomBackOff(3000);//ConcreteBackOffer.prewriteMaxBackoff
        byte[][] keys = new byte[keysList.size()][];
        keysList.toArray(keys);
        String prewriteError = this.prewriteKeys(prewriteBackoff, keys);
        if(prewriteError != null) {
            LOG.error("failed on prewrite, startTs={}, commitTs={}, detail={}", this.startTs, this.commitTs, prewriteError);
            return false;
        }
        TiTimestamp commitTso = kvClient.getTimestamp();
        this.commitTs = commitTso.getVersion();
        // check commitTS
        if(this.commitTs <= this.startTs) {
            LOG.error("invalid transaction tso with startTs={}, commitTs={}", this.startTs, this.commitTs);
            return false;
        }
        if(isExpired(this.startTs, maxTxnTimeUse)) {
            LOG.error("transaction takes too much time, startTs={}, commitTs={}", this.startTs, commitTso.getVersion());
            return false;
        }
        BackOffer commitBackoff =ConcreteBackOffer.newCustomBackOff(BackOffer.commitMaxBackoff);
        String commitError = this.commitKeys(commitBackoff, keys);
        if(commitError != null) {
            LOG.error("failed on commit, startTs={}, commitTs={}", this.startTs, commitError);
            return false;
        }

        return true;
    }

    private boolean isExpired(long lockTs, long maxTxnTimeUse) {
        return System.currentTimeMillis() >= TiTimestamp.extraPhysical(lockTs) + maxTxnTimeUse;
    }
}
