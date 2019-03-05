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

package org.tikv.common.memdb;

import org.tikv.common.memdb.iterator.IRetrieverMutator;

/**
 * MemBuffer is an in-memory kv collection, can be used to buffer write operations.
 */
public interface IMemBuffer extends IRetrieverMutator {

    // TxnEntrySizeLimit is limit of single entry size (len(key) + len(value)).
    int TxnEntrySizeLimit = 6 * 1024 * 1024;
    // TxnEntryCountLimit  is limit of number of entries in the MemBuffer.
    int TxnEntryCountLimit = 300 * 1000;
    // TxnTotalSizeLimit is limit of the sum of all entry size.
    int TxnTotalSizeLimit = 100 * 1024 * 1024;

    /**
     * Size returns sum of keys and values length.
     * @return
     */
    int size();

    /**
     * Len returns the number of entries in the DB.
     * @return
     */
    int length();

    /**
     * Reset cleanup the MemBuffer
     */
    void reset();

    /**
     * SetCap sets the MemBuffer capability, to reduce memory allocations.
     * Please call it before you use the MemBuffer, otherwise it will not works.
     * @param cap
     */
    void setcap(int cap);

}
