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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 *  UnionStore is a store that wraps a snapshot for read and a BufferStore for buffered write.
 *  Also, it provides some transaction related utilities.
 */
public class UnionStore implements IUnionStore, IMemBuffer{

    //private IMemBuffer memBuffer;

    private BufferStore bufferStore;

    private Snapshot snapshot;

    private Map<String, ConditionPair> lazyConditionPairs;

    private Map<Integer, Object> opts;

    public UnionStore(Snapshot st) {
        this.bufferStore = new BufferStore(st, BufferStore.DefaultTxnMembufCap);
        this.snapshot = st;
        this.lazyConditionPairs = new HashMap<>();
        this.opts = new HashMap<>();
        //this.memBuffer = new MemDbBuffer(BufferStore.DefaultTxnMembufCap);
    }

    @Override
    public IMemBuffer getMemBuffer() {
        return this.bufferStore;
    }

    @Override
    public Snapshot getSnapshot() {
        return null;
    }

    @Override
    public ConditionPair lookupConditionPair(Key key) {
        return this.lazyConditionPairs.get(key.toString());
    }

    @Override
    public boolean walkBuffer(Function<Map.Entry<Key, Key>, String> fun) {
        //throw new RuntimeException("Method not supported, no implementation");
        return bufferStore.walkBuffer(fun);
    }

    @Override
    public int size() {
        throw new RuntimeException("Method not supported, no implementation");
        //return 0;
    }

    @Override
    public int length() {
        throw new RuntimeException("Method not supported, no implementation");
        //return 0;
    }

    @Override
    public void reset() {
        this.bufferStore.reset();
    }

    @Override
    public void setcap(int cap) {
        this.bufferStore.setcap(cap);
    }

    @Override
    public boolean set(Key key, byte[] value) {
        return this.bufferStore.set(key, value);
    }

    @Override
    public boolean delete(Key key) {
        //throw new RuntimeException("Method not supported, no implementation");
        return this.bufferStore.delete(key);
    }

    @Override
    public byte[] get(Key key) {
        byte[] val = this.bufferStore.get(key);
        if(val == null) {
            val = this.snapshot.get(key);
        }
        if(val == null || val.length == 0) {
            return null;
        }
        return val;
    }

    @Override
    public Iterator iterator(Key key, Key upperBound) {
        //
        return this.bufferStore.iterator(key, upperBound);
    }

    @Override
    public Iterator iteratorReverse(Key key) {
        //
        return this.bufferStore.iteratorReverse(key);
    }

    protected void markLazyConditionPair(Key key, byte[] val, String error) {
        ConditionPair pair = new ConditionPair();
        pair.setKey(Key.toRawKey(key.getBytes()));
        pair.setValue(val);
        pair.setError(error);
        this.lazyConditionPairs.put(key.toString(), pair);
    }
}
