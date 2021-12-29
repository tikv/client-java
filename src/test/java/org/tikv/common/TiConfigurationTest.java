/*
 * Copyright 2021 PingCAP, Inc.
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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.junit.Test;
import org.tikv.BaseRawKVTest;

public class TiConfigurationTest extends BaseRawKVTest {

  @Test
  public void configFileTest() {
    TiConfiguration conf = createTiConfiguration();
    assertEquals("configFileTest", conf.getDBPrefix());
  }

  @Test
  public void testGrpcIdleTimeoutValue() {
    TiConfiguration conf = createTiConfiguration();
    // default value
    assertEquals(TiConfiguration.getInt(ConfigUtils.TIKV_GRPC_IDLE_TIMEOUT), conf.getIdleTimeout());
    // new value
    int newValue = 100000;
    conf.setIdleTimeout(newValue);
    assertEquals(newValue, conf.getIdleTimeout());
  }

  @Test
  public void slowLogDefaultValueTest() {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    assertEquals(conf.getTimeout() * 2, conf.getRawKVReadSlowLogInMS().longValue());
    assertEquals(conf.getTimeout() * 2, conf.getRawKVWriteSlowLogInMS().longValue());
    assertEquals(conf.getTimeout() * 2, conf.getRawKVBatchReadSlowLogInMS().longValue());
    assertEquals(conf.getTimeout() * 2, conf.getRawKVBatchWriteSlowLogInMS().longValue());
  }

  @Test
  public void serializeTest() throws IOException {
    TiConfiguration conf = TiConfiguration.createDefault();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(conf);
      oos.flush();
    }
  }
}
