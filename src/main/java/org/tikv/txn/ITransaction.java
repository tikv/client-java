package org.tikv.txn;

import org.tikv.common.Snapshot;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTimestamp;

import java.util.Iterator;

/**
 * definition of Transaction api
 */
public interface ITransaction {

    boolean set(byte[] key, byte[] value);

    byte[] get(byte[] key);

    boolean delete(byte[] key);

    Iterator<byte[]> iterator(byte[] startKey, byte[] endKey);

    Iterator<byte[]> iteratorReverse(byte[] startKey);

    /**
     * create TwoPhaseCommitter, and call 2pc api
     * @return
     */
    boolean commit();

    boolean rollback();

    boolean lockKeys(Key ...lockedKeys);

    /**
     *
     * @return returns if the transaction is valid
     */
    boolean valid();

    long getStartTS();

    boolean isReadOnly();

    Snapshot getSnapshot(TiTimestamp timestamp);
}
