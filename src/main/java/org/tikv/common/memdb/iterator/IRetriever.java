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

package org.tikv.common.memdb.iterator;

import org.tikv.common.key.Key;

import java.util.Iterator;

/**
 *  Retriever is the interface wraps the basic Get and Seek methods.
 */
public interface IRetriever {

    /**
     *  Get gets the value for key k from kv store.
     * 	If corresponding kv pair does not exist, it returns null.
     * @param key
     * @return
     */
    byte[] get(Key key);

    /**
     * Iter creates an Iterator positioned on the first entry that k <= entry's key.
     * If such entry is not found, it returns an invalid Iterator with no error.
     * @param key
     * @param upperBound
     * @return
     */
    Iterator iterator(Key key, Key upperBound);

    /**
     * IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
     * The returned iterator will iterate from greater key to smaller key.
     * If k is nil, the returned iterator will be positioned at the last key.
     * @param key
     * @return
     */
    Iterator iteratorReverse(Key key);
}
