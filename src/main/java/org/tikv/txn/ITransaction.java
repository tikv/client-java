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

import org.tikv.common.Snapshot;
import org.tikv.common.key.Key;

import java.util.List;
import java.util.Map;

/**
 * definition of Transaction api
 */
public interface ITransaction {

    boolean set(byte[] key, byte[] value);

    byte[] get(byte[] key);

    boolean delete(byte[] key);

    //Iterator<byte[]> iterator(byte[] startKey, byte[] endKey);

    //Iterator<byte[]> iteratorReverse(byte[] startKey);

    /**
     * create TwoPhaseCommitter, and call 2pc api
     * @return
     */
    boolean commit();

    boolean rollback();

    boolean lockKeys(Key... lockedKeys);
    /**
     *
     * @return returns if the transaction is valid
     */
    boolean valid();

    long getStartTS();

    long getStartTime();

    boolean isReadOnly();

    Snapshot getSnapshot();

    TxnKVClient getKVClient();

    Map<byte[], byte[]> getStoredKeys();

    List<byte[]> getLockedKeys();
}
