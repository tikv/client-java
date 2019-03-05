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

/**
 * Mutator is the interface wraps the basic Set and Delete methods.
 */
public interface IMutator {

    boolean set(Key key, byte[] value);

    boolean delete(Key key);
}
