/*
 * Copyright 2021 TiKV Project Authors.
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

package org.tikv.raw;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.tikv.BaseRawKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Errorpb;

public class WarmupTest extends BaseRawKVTest {

  @Test
  public void testErrorpbToStringEnableWarmup() throws Exception {
    TiConfiguration conf = createTiConfiguration();
    TiSession session = TiSession.create(conf);

    long start = System.currentTimeMillis();
    Errorpb.Error.newBuilder()
        .setNotLeader(Errorpb.NotLeader.newBuilder().build())
        .build()
        .toString();
    long end = System.currentTimeMillis();
    assertTrue(end - start < 10);
    session.close();
  }

  @Test
  public void testErrorpbToStringDisableWarmup() throws Exception {
    TiConfiguration conf = createTiConfiguration();
    conf.setWarmUpEnable(false);
    TiSession session = TiSession.create(conf);

    long start = System.currentTimeMillis();
    Errorpb.Error.newBuilder()
        .setNotLeader(Errorpb.NotLeader.newBuilder().build())
        .build()
        .toString();
    long end = System.currentTimeMillis();
    assertTrue(end - start > 10);
    session.close();
  }
}
