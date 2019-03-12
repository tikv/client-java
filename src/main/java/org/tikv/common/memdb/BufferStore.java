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

import org.tikv.common.Snapshot;
import org.tikv.common.key.Key;
import org.tikv.common.memdb.iterator.IMutator;
import org.tikv.common.memdb.iterator.IRetriever;
import org.tikv.common.memdb.iterator.UnionStoreIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class BufferStore implements IMemBuffer{
    // DefaultTxnMembufCap is the default transaction membuf capability.
    public static final int DefaultTxnMembufCap = 4 * 1024;

    private IRetriever retriever;

    private IMemBuffer memBuffer;

    public BufferStore(Snapshot r, int cap) {
        if(cap < 0) {
            cap = DefaultTxnMembufCap;
        }
        this.retriever = r;
        this.memBuffer = new LazyMemBuffer(cap);
    }

    @Override
    public int size() {
        return this.memBuffer.size();
    }

    @Override
    public int length() {
        return this.memBuffer.length();
    }

    @Override
    public void reset() {
        this.memBuffer.reset();
    }

    @Override
    public void setcap(int cap) {
        this.memBuffer.setcap(cap);
    }

    @Override
    public boolean set(Key key, byte[] value) {
        //memBuffer
        return this.memBuffer.set(key, value);
    }

    @Override
    public boolean delete(Key key) {
        //memBuffer
        return this.memBuffer.delete(key);
    }

    /**
     * Get implements the Retriever interface.
     * @param key
     * @return
     */
    @Override
    public byte[] get(Key key) {
        byte[] val = this.memBuffer.get(key);
        if(val == null) {
            val = this.retriever.get(key);
        }
        if(val == null || val.length == 0) {
            return null;
        }
        return val;
    }

    /**
     * Iter implements the Retriever interface.
     * @param key
     * @param upperBound
     * @return
     */
    @Override
    public Iterator iterator(Key key, Key upperBound) {
        Iterator bufferIt = this.memBuffer.iterator(key, upperBound);
        Iterator snapshotIt = this.retriever.iterator(key, upperBound);
        return new UnionStoreIterator(bufferIt, snapshotIt);
    }

    @Override
    public Iterator iteratorReverse(Key key) {
        Iterator bufferIt = this.memBuffer.iteratorReverse(key);
        Iterator snapshotIt = this.retriever.iteratorReverse(key);
        return new UnionStoreIterator(bufferIt, snapshotIt, true);
    }

    /**
     * WalkBuffer iterates all buffered kv pairs.
     * @param f
     * @return
     */
    public boolean walkBuffer(Function<Map.Entry<Key, Key>, String> f) {
        Iterator iterator = this.memBuffer.iterator(null, null);
        if(iterator != null) {
            while(iterator.hasNext()) {
                Map.Entry<Key, Key> entry = (Map.Entry<Key, Key>) iterator.next();
                String error = f.apply(entry);
                if(error == null) {
                    continue;
                }
                //error happened
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * SaveTo saves all buffered kv pairs into a Mutator.
     * @param mutator
     * @return
     */
    public boolean saveTo(IMutator mutator) {
        walkBuffer(entry -> {
            Key key = entry.getKey();
            Key val = entry.getValue();
            byte[] value = val.getBytes();
            if(value.length == 0) {
                boolean deleted = mutator.delete(key);
                return deleted ? null : "delete [" + key.toString() + "] failed";
            }
            boolean inserted = mutator.set(key, value);
            return inserted ? null : "set [" + key.toString() + "," + val.toString() + "] failed";
        });
        return false;
    }
}
