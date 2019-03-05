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

import org.tikv.common.key.Key;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * implements the MemBuffer interface.
 */
public class MemDbBuffer implements IMemBuffer{
    private final ConcurrentSkipListMap<Key, Key> db;

    private int entrySizeLimit;

    private int bufferLenLimit;

    private int bufferSizeLimit;

    private int capacity;

    public MemDbBuffer(int cap) {
        this.db = new ConcurrentSkipListMap<>();
        this.capacity = cap;
        this.entrySizeLimit = TxnEntrySizeLimit;
        this.bufferLenLimit = TxnEntryCountLimit;
        this.bufferSizeLimit = TxnTotalSizeLimit;
    }

    @Override
    public int size() {
        return this.db.keySet().size() + this.db.values().size();
    }

    @Override
    public int length() {
        return this.db.size();
    }

    @Override
    public void reset() {
        this.db.clear();
    }

    @Override
    public void setcap(int cap) {
        this.capacity = cap;
    }

    @Override
    public boolean set(Key key, byte[] value) {
        if(value == null || value.length == 0) {
            //ErrCannotSetNilValue
            return false;
        }
        if(value.length + key.getBytes().length > this.entrySizeLimit) {
            //ErrEntryTooLarge
            return false;
        }
        boolean success = true;
        try {
            this.db.put(key, Key.toRawKey(value));
        } catch (Exception e) {
            success = false;
        }
        if(this.size() > this.bufferSizeLimit) {
            //ErrTxnTooLarge
            return false;
        }
        if(this.length() > this.bufferLenLimit) {
            //ErrTxnTooLarge
            return false;
        }
        return success;
    }

    @Override
    public boolean delete(Key key) {
        try {
            this.db.remove(key);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public byte[] get(Key key) {
        Key value = this.db.get(key);
        byte[] valueBytes = value != null && value.getBytes().length > 0 ? value.getBytes() : null;
        return valueBytes;
    }

    @Override
    public Iterator iterator(Key key, Key upperBound) {
        if(key == null && upperBound == null) {
            return this.db.entrySet().iterator();
        }
        if(key == null || upperBound == null) {
            //invalid iterator params
            return null;
        }
        NavigableMap<Key, Key> subMap = this.db.subMap(key, upperBound);
        return subMap.entrySet().iterator();
    }

    @Override
    public Iterator iteratorReverse(Key key) {
        NavigableMap<Key, Key> subMap = this.db.descendingMap().headMap(key);
        return subMap.entrySet().iterator();
    }
}
