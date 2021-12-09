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

import org.junit.Test;

public class TiConfigurationTest {

  @Test
  public void configFileTest() {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    assertEquals("configFileTest", conf.getDBPrefix());
  }

  @Test
  public void testGrpcIdleTimeoutValue() {
    TiConfiguration conf = TiConfiguration.createDefault();
    // default value
    assertEquals(TiConfiguration.getInt(ConfigUtils.TIKV_GRPC_IDLE_TIMEOUT), conf.getIdleTimeout());
    // new value
    int newValue = 100000;
    conf.setIdleTimeout(newValue);
    assertEquals(newValue, conf.getIdleTimeout());
  }
}
