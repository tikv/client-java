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
import org.junit.After;
import org.junit.Before;

public abstract class PDMockServerTest {
  protected static final String LOCAL_ADDR = "127.0.0.1";
  static final long CLUSTER_ID = 1024;
  protected static TiSession session;
  protected PDMockServer pdServer;

  @Before
  public void setUp() throws IOException {
    setUp(LOCAL_ADDR);
  }

<<<<<<< HEAD
  void setUp(String addr) throws IOException {
    pdServer = new PDMockServer();
    pdServer.start(CLUSTER_ID);
    pdServer.addGetMemberResp(
        GrpcUtils.makeGetMembersResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeMember(1, "http://" + addr + ":" + pdServer.port),
            GrpcUtils.makeMember(2, "http://" + addr + ":" + (pdServer.port + 1)),
            GrpcUtils.makeMember(3, "http://" + addr + ":" + (pdServer.port + 2))));
    TiConfiguration conf = TiConfiguration.createDefault(addr + ":" + pdServer.port);
    conf.setEnableGrpcForward(false);
=======
  void setup(String addr) throws IOException {
    int basePort;
    try (ServerSocket s = new ServerSocket(0)) {
      basePort = s.getLocalPort();
    }

    for (int i = 0; i < 3; i++) {
      PDMockServer server = new PDMockServer();
      server.start(CLUSTER_ID, basePort + i);
      server.addGetMembersListener(
          (request) ->
              GrpcUtils.makeGetMembersResponse(
                  server.getClusterId(),
                  GrpcUtils.makeMember(1, "http://" + addr + ":" + basePort),
                  GrpcUtils.makeMember(2, "http://" + addr + ":" + (basePort + 1)),
                  GrpcUtils.makeMember(3, "http://" + addr + ":" + (basePort + 2))));
      pdServers.add(server);
      if (i == 0) {
        leader = server;
      }
    }

    TiConfiguration conf = TiConfiguration.createDefault(addr + ":" + leader.port);
>>>>>>> e89ca5f37... [close #550] rawkv: fix seek leader/proxy store early abort (#551)
    conf.setKvMode("RAW");
    conf.setTest(true);
    conf.setTimeout(2000);
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    pdServer.stop();
  }
}
