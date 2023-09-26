/*
 * Copyright 2020 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.tikv.common.TiConfiguration.ApiVersion;

public abstract class PDMockServerTest {
  protected static final String LOCAL_ADDR = "127.0.0.1";
  static final long CLUSTER_ID = 1024;
  protected TiSession session;
  protected PDMockServer leader;
  protected List<PDMockServer> pdServers = new ArrayList<>();

  @Before
  public void setup() throws IOException {
    setup(LOCAL_ADDR);
  }

  void upgradeToV2Cluster() throws Exception {
    if (session == null) {
      throw new IllegalStateException("Cluster is not initialized");
    }

    if (session.getConf().getApiVersion().isV2()) {
      return;
    }

    TiConfiguration conf = session.getConf().setApiVersion(ApiVersion.V2);
    session.close();

    session = TiSession.create(conf);
  }

  void updateConf(Function<TiConfiguration, TiConfiguration> update) throws Exception {
    if (session == null) {
      throw new IllegalStateException("Cluster is not initialized");
    }

    session.close();

    session = TiSession.create(update.apply(session.getConf()));
  }

  void setup(String addr) throws IOException {
    int[] ports = new int[3];
    for (int i = 0; i < ports.length; i++) {
      ports[i] = GrpcUtils.getFreePort();
    }

    for (int i = 0; i < ports.length; i++) {
      PDMockServer server = new PDMockServer();
      server.start(CLUSTER_ID, ports[i]);
      server.addGetMembersListener(
          (request) ->
              GrpcUtils.makeGetMembersResponse(
                  server.getClusterId(),
                  GrpcUtils.makeMember(1, "http://" + addr + ":" + ports[0]),
                  GrpcUtils.makeMember(2, "http://" + addr + ":" + ports[1]),
                  GrpcUtils.makeMember(3, "http://" + addr + ":" + ports[2])));
      pdServers.add(server);
      if (i == 0) {
        leader = server;
      }
    }

    TiConfiguration conf = TiConfiguration.createDefault(addr + ":" + ports[0]);
    conf.setKvMode("RAW");
    conf.setWarmUpEnable(false);
    conf.setTimeout(20000);
    conf.setRawKVReadTimeoutInMS(2000);
    conf.setEnableGrpcForward(true);

    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    for (PDMockServer server : pdServers) {
      server.stop();
    }
  }
}
