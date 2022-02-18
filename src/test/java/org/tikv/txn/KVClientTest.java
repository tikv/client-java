/*
 * Copyright 2018 TiKV Project Authors.
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

package org.tikv.txn;

import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class KVClientTest {
  private TiSession session;

  @Before
  public void setUp() {
    TiConfiguration conf = TiConfiguration.createDefault();
    conf.setTest(true);
    try {
      session = TiSession.create(conf);
    } catch (Exception e) {
      fail("TiKV cluster may not be present");
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Ignore
  public void unsafeDestroyRangeTest() {
    try (KVClient client = session.createKVClient()) {
      client.unsafeDestoryRange(ByteString.EMPTY, ByteString.EMPTY);
    }
  }
}
