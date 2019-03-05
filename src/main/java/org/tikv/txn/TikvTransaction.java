/*
 * Copyright 2019 The TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.txn;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.Snapshot;
import org.tikv.common.key.Key;
import org.tikv.common.memdb.MemDbBuffer;
import org.tikv.common.memdb.UnionStore;
import org.tikv.common.meta.TiTimestamp;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Transaction implementation of TiKV client
 */
public class TikvTransaction implements ITransaction {
    private static final Logger LOG = LoggerFactory.getLogger(TikvTransaction.class);

    private TxnKVClient kvClient;
    /**
     * start timestamp of transaction which get from PD
     */
    private long startTS;
    /**
     * Monotonic timestamp for recording txn time consuming.
     */
    private long startTime; //for recording txn time consuming.
    /**
     * transaction valid flag
     */
    private boolean valid;

    //private ReentrantMutex mutex = new ReentrantMutex();

    //private Map<byte[], byte[]> memoryKvStore = new HashMap<>();

    //private MemDbBuffer memDb = new MemDbBuffer(1000);

    private final UnionStore memdb;

    private List<byte[]> lockKeys;

    private Snapshot snapshot;

    private final Function<ITransaction, Boolean> transactionFunction;

    private static final int retryBackOffCap = 100;
    private static final int retryBackOffBase = 1;
    private static final int maxRetryCnt = 100;

    private SecureRandom random = new SecureRandom();

    public TikvTransaction(TxnKVClient client) {
        this(client, null);
    }

    public TikvTransaction(TxnKVClient client, Function<ITransaction, Boolean> function) {
        this.kvClient = client;
        this.startTime = System.currentTimeMillis();
        this.transactionFunction = function;
        this.lockKeys = Lists.newLinkedList();
        this.init();
        this.memdb = new UnionStore(this.snapshot);
    }

    private void init() {
        this.valid = true;
        TiTimestamp tiTimestamp = kvClient.getTimestamp();
        this.startTS = tiTimestamp.getVersion();
        this.snapshot = new Snapshot(kvClient.getConf(), kvClient.getPdClient(), kvClient.getClientBuilder(), tiTimestamp);
    }

    @Override
    public boolean set(byte[] key, byte[] value) {
        memdb.set(Key.toRawKey(key), value);
        return true;
    }

    @Override
    public byte[] get(byte[] key) {
        byte[] value = memdb.get(Key.toRawKey(key));
        if(value != null) {
            return value;
        }
        return snapshot.get(key);
    }

    @Override
    public boolean delete(byte[] key) {
        memdb.set(Key.toRawKey(key), new byte[0]);
        return true;
    }

    @Override
    public boolean commit() {
        boolean result;
        if(this.transactionFunction != null) {
            //commit with restart execute txn when encountered write conflict;
            result = this.commitWithRetry();
        } else {
            TwoPhaseCommitter committer = new TwoPhaseCommitter(this);
            // latches enabled
            // for transactions which need to acquire latchess
            //TODO latch ??
            result = committer.execute();
        }
        long endTime = System.currentTimeMillis();
        LOG.debug("txn startTime at {}, endTime at {}, spend whole time {}s", this.startTime, (endTime - this.startTime) / 1000);
        return result;
    }

    private boolean commitWithRetry() {
        for(int i = 0 ; i < maxRetryCnt; i++) {
            Function<ITransaction, Boolean> retryFunction = transactionFunction;
            Boolean result = retryFunction.apply(this);
            if(!result) {
                this.rollback();
                continue;
            }

            TwoPhaseCommitter committer = new TwoPhaseCommitter(this);
            boolean commit = committer.execute();
            if(commit) {
                return true;
            }
            this.lockKeys.clear();
            this.memdb.reset();
            this.init();
            LOG.warn("txn commit failed with attempts {} times, startTs={}", i + 1, startTime);
            backoff(i);
        }
        LOG.warn("txn commit failed at finally, startTs={}", startTime);
        return false;
    }

    @Override
    public boolean rollback() {
        if(!this.valid) {
            LOG.warn("rollback invalid, startTs={}, startTime={}", this.startTS, this.startTime);
            return false;
        }
        this.close();
        LOG.debug("transaction rollback, startTs={}, startTime={}", this.startTS, this.startTime);
        return true;
    }

    @Override
    public boolean lockKeys(Key... lockedKeys) {
        for(Key key : lockedKeys) {
            this.lockKeys.add(key.toByteString().toByteArray());
        }
        return true;
    }

    @Override
    public boolean valid() {
        return this.valid;
    }

    @Override
    public long getStartTS() {
        return this.startTS;
    }

    @Override
    public long getStartTime() {
        return this.startTime;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Snapshot getSnapshot() {
        return this.snapshot;
    }

    @Override
    public TxnKVClient getKVClient() {
        return this.kvClient;
    }

    @Override
    public Map<byte[], byte[]> getStoredKeys() {
        Iterator it = memdb.iterator(null, null);
        Map<byte[], byte[]> result = new HashMap<>();
        while(it != null && it.hasNext()) {
            Map.Entry<Key, Key> entry = (Map.Entry<Key, Key>)it.next();
            result.put(entry.getKey().getBytes(), entry.getValue().getBytes());
        }
        return result;
    }

    @Override
    public List<byte[]> getLockedKeys() {
        return this.lockKeys;
    }

    private void close() {
        this.valid = false;
        this.lockKeys.clear();
        this.memdb.reset();
    }

    // BackOff Implements exponential backoff with full jitter.
    // Returns real back off time in microsecond.
    // See http://www.awsarchitectureblog.com/2015/03/backoff.html.
    private int backoff(int attempts) {
        int upper = (int)(Math.min(retryBackOffCap, retryBackOffBase * Math.pow(2.0, attempts)));
        int sleep = random.nextInt(upper);
        try {
            Thread.sleep(sleep);
            LOG.debug("txn sleep {}s at attempts " , sleep, attempts);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sleep;
    }
}
