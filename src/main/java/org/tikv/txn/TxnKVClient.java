package org.tikv.txn;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.txn.pool.SecondaryCommitTaskThreadPool;
import org.tikv.txn.type.BatchKeys;
import org.tikv.txn.type.GroupKeyResult;
import org.tikv.txn.type.TwoPhaseCommitType;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TxnKVClient {
    private final static Logger LOG = LoggerFactory.getLogger(TxnKVClient.class);

    // TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
    // Key+Value size below 16KB.
    private static final int txnCommitBatchSize = 16 * 1024;
    private static final SecondaryCommitTaskThreadPool secondaryCommitPool = new SecondaryCommitTaskThreadPool();

    private Map<String, Kvrpcpb.Mutation> mutations = new HashMap<>();
    private List<byte[]> keysList;
    private ReadOnlyPDClient pdClient;
    private RegionStoreClient regionStoreClient;

    private volatile boolean prewriteTaskError = false;
    private volatile AtomicInteger seondaryThreadIdGenerator = new AtomicInteger(0);


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
                    List<byte[]> groupItem = groups.get(regionId);
                    if(groupItem != null) {
                        groupItem = new LinkedList<>();
                    }
                    groupItem.add(key);
                    groups.put(tiRegion.getId(), groupItem);
                }
            }
        } catch (Exception e) {
            error = String.format("groupKeysByRegion error, %s", e.getMessage());
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
        int start, end ;
        int len = keys.size();
        for(start = 0, end =0; start < len; start = end) {
            int size = 0;
            for(end = start; end < len && size < limit; end ++) {
                if(sizeKeyValue) {
                    size += this.keyValueSize(keys.get(end));
                } else {
                    size += this.keyize(keys.get(end));
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
        this.appendBatchBySize(batchKeyList, firstRegion, groupKeyMap.get(firstRegion),
                sizeKeyValue, txnCommitBatchSize);

        for(Long regionId : groupKeyMap.keySet()) {
            this.appendBatchBySize(batchKeyList, regionId, groupKeyMap.get(regionId),
                    sizeKeyValue, txnCommitBatchSize);
        }
        boolean firstIsPrimary =  this.isPrimaryKey(keys[0]);

        if (firstIsPrimary && (actionType == TwoPhaseCommitType.actionCommit
                || actionType == TwoPhaseCommitType.actionCleanup)) {
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
                    LOG.error("2PC async doActionOnBatches canceled, other secondary thread failed");
                    return;
                }
                String errorInner = doActionOnBatches(backOffer, actionType, batchKeyList);
                if(errorInner != null) {
                    LOG.warn("2PC async doActionOnBatches error: {}", errorInner);
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
            LOG.debug("2PC doActionOnBatches batch keys is empty: type={}", actionType);
            return null;
        }
        switch(actionType) {
            case actionPrewrite: {
                this.doPrewriteActionOnBatches(backOffer, batchKeys);
            } break;
            case actionCommit: {
                this.doCommitActionOnBatches(backOffer, batchKeys);
            } break;
            case actionCleanup: {
                this.doCleanupActionOnBatches(backOffer, batchKeys);
            } break;
        }
        return null;
    }

    private String doPrewriteActionOnBatches(BackOffer backOffer, List<BatchKeys> batchKeysList) {
        if(batchKeysList.size() == 1) {
            String error = this.prewriteSingleBatch(backOffer, batchKeysList.get(0));
            LOG.warn("2PC doPrewriteActionOnBatches failed, one batch size, error: {}", error);
            return error;
        }
        // For prewrite, stop sending other requests after receiving first error.
        for(BatchKeys batchKeys : batchKeysList) {
            String error = prewriteSingleBatch(backOffer, batchKeys);
            if(error != null) {
                //single to other thread error happened
                prewriteTaskError = true;
                LOG.warn("2PC doPrewriteActionOnBatches failed, send to other request: {}", error);
                return error;
            }
        }
        return null;
    }

    private String doCommitActionOnBatches(BackOffer backOffer, List<BatchKeys> batchKeysList) {
        if(batchKeysList.size() == 1) {
            String error = this.prewriteSingleBatch(backOffer, batchKeysList.get(0));
            LOG.warn("2PC doCommitActionOnBatches failed, one batch size, error: {}", error);
            return error;
        }
        // For prewrite, stop sending other requests after receiving first error.
        for(BatchKeys batchKeys : batchKeysList) {
            String error = prewriteSingleBatch(backOffer, batchKeys);
            if(error != null) {
                LOG.warn("2PC doCommitActionOnBatches failed, send to other request: {}", error);
                return error;
            }
        }
        return null;
    }

    private String doCleanupActionOnBatches(BackOffer backOffer, List<BatchKeys> batchKeysList) {
        if(batchKeysList.size() == 1) {
            String error = this.prewriteSingleBatch(backOffer, batchKeysList.get(0));
            LOG.warn("2PC doCleanupActionOnBatches failed, one batch size, error: {}", error);
            return error;
        }
        // For prewrite, stop sending other requests after receiving first error.
        for(BatchKeys batchKeys : batchKeysList) {
            String error = prewriteSingleBatch(backOffer, batchKeys);
            if(error != null) {
                //single to other thread error happened
                prewriteTaskError = true;
                LOG.warn("2PC doCleanupActionOnBatches failed, send to other request: {}", error);
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

    public long keyize(byte[] key) {
        return key.length;
    }

    public String prewriteSingleBatch(BackOffer backOffer, BatchKeys batchKeys) {
        List<byte[]> keyList = batchKeys.getKeys();
        int batchSize = keyList.size();
        List<Kvrpcpb.Mutation> mutationList = new ArrayList<>(batchSize);
        for(byte[] key : keyList) {
            mutationList.add(mutations.get(new String(key)));
        }
        return null;
    }

    public String commitSingleBatch(BackOffer backOffer, BatchKeys batchKeys) {

        return null;
    }

    public String cleanupSingleBatch(BackOffer backOffer, BatchKeys batchKeys) {

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
}
