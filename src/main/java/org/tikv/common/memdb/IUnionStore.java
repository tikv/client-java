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

import java.util.Map;
import java.util.function.Function;

/**
 *
 */
public interface IUnionStore {

    /**
     * GetMemBuffer return the MemBuffer binding to this UnionStore.
     * @return
     */
    IMemBuffer getMemBuffer();

    /**
     * Returns related condition pair
     * @param key
     * @return
     */
    ConditionPair lookupConditionPair(Key key);

    /**
     * WalkBuffer iterates all buffered kv pairs.
     */
    void walkBuffer(Function<Map.Entry<Key, Key>, String> fun);

    class ConditionPair {

        private Key key;

        private byte[] value;

        private String error;

        public Key getKey() {
            return key;
        }

        public void setKey(Key key) {
            this.key = key;
        }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }
    }
}
