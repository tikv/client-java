package org.tikv.txn;

import com.google.common.collect.Lists;
import org.tikv.common.Snapshot;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTimestamp;

import java.util.Iterator;
import java.util.List;

/**
 * @author jackycchen
 */
public class TikvTransaction implements ITransaction {

    private TxnKVClient kvClient;
    /**
     * start timestamp of transaction which get from PD
     */
    private long startTS;
    /**
     * commit timestamp of transaction which get from PD
     */
    private long commitTS;
    /**
     * Monotonic timestamp for recording txn time consuming.
     */
    private long startTime; //for recording txn time consuming.
    /**
     * transaction valid flag
     */
    private boolean valid;
    private List<byte[]> lockKeys;
    private Snapshot snapshot;

    public TikvTransaction(TxnKVClient client) {
        this.kvClient = client;
        lockKeys = Lists.newLinkedList();
        valid = true;
    }

    @Override
    public boolean set(byte[] key, byte[] value) {
        return false;
    }

    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }

    @Override
    public boolean delete(byte[] key) {
        return false;
    }

    @Override
    public Iterator<byte[]> iterator(byte[] startKey, byte[] endKey) {
        return null;
    }

    @Override
    public Iterator<byte[]> iteratorReverse(byte[] startKey) {
        return null;
    }

    @Override
    public boolean commit() {
        return false;
    }

    @Override
    public boolean rollback() {
        return false;
    }

    @Override
    public boolean lockKeys(Key... lockedKeys) {
        return false;
    }

    @Override
    public boolean valid() {
        return false;
    }

    @Override
    public long getStartTS() {
        return 0;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Snapshot getSnapshot(TiTimestamp timestamp) {
        return this.snapshot;
    }
}
