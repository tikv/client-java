package org.tikv.txn;

import com.google.common.collect.Lists;
import com.sun.corba.se.impl.orbutil.concurrent.ReentrantMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.Snapshot;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTimestamp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private ReentrantMutex mutex = new ReentrantMutex();

    private Map<byte[], byte[]> memoryKvStore = new HashMap<>();

    private List<byte[]> lockKeys;

    private Snapshot snapshot;

    public TikvTransaction(TxnKVClient client) {
        this.kvClient = client;
        this.lockKeys = Lists.newLinkedList();
        this.valid = true;
        TiTimestamp tiTimestamp = kvClient.getTimestamp();
        this.startTS = tiTimestamp.getVersion();
        this.startTime = System.currentTimeMillis();
        this.snapshot = new Snapshot(tiTimestamp, client.getSession());
    }

    @Override
    public boolean set(byte[] key, byte[] value) {
        memoryKvStore.put(key, value);
        return true;
    }

    @Override
    public byte[] get(byte[] key) {
        if(memoryKvStore.get(key) != null) {
            return memoryKvStore.get(key);
        }
        return snapshot.get(key);
    }

    @Override
    public boolean delete(byte[] key) {
        memoryKvStore.put(key, new byte[0]);
        return true;
    }

    @Override
    public boolean commit() {
        TwoPhaseCommitter committer = new TwoPhaseCommitter(this);
        // latches enabled
        // for transactions which need to acquire latchess
        //TODO latch ??
        return committer.execute();
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
        return memoryKvStore;
    }

    @Override
    public List<byte[]> getLockedKeys() {
        return this.lockKeys;
    }

    private void close() {
        this.valid = false;
        this.lockKeys.clear();
        this.memoryKvStore.clear();
    }

    private byte[][] toKeys() {
        byte[][] keys = new byte[memoryKvStore.size()][];
        int i = 0;
        for(byte[] key : memoryKvStore.keySet()) {
            keys[i++] = key;
        }
        return keys;
    }
}
