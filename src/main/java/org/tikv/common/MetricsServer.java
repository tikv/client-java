/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tikv.common;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsServer {
  private static final Logger logger = LoggerFactory.getLogger(MetricsServer.class);

  private static MetricsServer METRICS_SERVER_INSTANCE = null;
  private static int metricsServerRefCount = 0;

  private final int port;
  private final HTTPServer server;

  public static MetricsServer getInstance(TiConfiguration conf) {
    if (!conf.isMetricsEnable()) {
      return null;
    }

    synchronized (MetricsServer.class) {
      int port = conf.getMetricsPort();
      if (METRICS_SERVER_INSTANCE != null) {
        if (port != METRICS_SERVER_INSTANCE.port) {
          throw new IllegalArgumentException(
              String.format(
                  "Do dot support multiple tikv.metrics.port, which are %d and %d",
                  port, METRICS_SERVER_INSTANCE.port));
        }
      } else {
        METRICS_SERVER_INSTANCE = new MetricsServer(port);
      }
      metricsServerRefCount += 1;
      return METRICS_SERVER_INSTANCE;
    }
  }

  private MetricsServer(int port) {
    try {
      this.port = port;
      DefaultExports.initialize();
      this.server = new HTTPServer(port, true);
      logger.info("http server is up " + this.server.getPort());
    } catch (Exception e) {
      logger.error("http server not up");
      throw new RuntimeException(e);
    }
  }

  public void close() {
    synchronized (MetricsServer.class) {
      if (metricsServerRefCount == 1) {
        if (server != null) {
          server.stop();
          logger.info("Metrics server on " + server.getPort() + " is stopped");
        }
        METRICS_SERVER_INSTANCE = null;
      }

      if (metricsServerRefCount >= 1) {
        metricsServerRefCount -= 1;
      }
    }
  }
}
