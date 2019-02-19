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
