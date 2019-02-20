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

import org.tikv.common.TiSession;
import org.tikv.txn.gc.GCWorker;

public class TxnKVClient {
  // TODO: To be done.
  public TxnKVClient(TiSession session) {
    GCWorker worker = new GCWorker(session);
    worker.run();
  }
}
