package org.tikv.common;

import java.util.concurrent.atomic.AtomicLong;

public interface ClusterIdRecorder {
  AtomicLong clusterId = new AtomicLong(0);

  default void withClusterId(long id) {
    clusterId.set(id);
  }

  default Long getClusterId() {
    return clusterId.get();
  }
}
