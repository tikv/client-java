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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.BaseRawKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.RawCASConflictException;

public class CASTest extends BaseRawKVTest {

  private RawKVClient client;
  private boolean initialized;
  private static final Logger logger = LoggerFactory.getLogger(RawKVClientTest.class);
  private TiSession session;

  @Before
  public void setup() {
    try {
      TiConfiguration conf = createTiConfiguration();
      conf.setEnableAtomicForCAS(true);
      session = TiSession.create(conf);
      initialized = false;
      if (client == null) {
        client = session.createRawClient();
      }
      initialized = true;
    } catch (Exception e) {
      logger.warn(
          "Cannot initialize raw client, please check whether TiKV is running. Test skipped.", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void rawCASTest() {
    ByteString key = ByteString.copyFromUtf8("key_atomic");
    ByteString value = ByteString.copyFromUtf8("value");
    ByteString value2 = ByteString.copyFromUtf8("value2");
    client.delete(key);
    client.compareAndSet(key, Optional.empty(), value);
    Assert.assertEquals(value, client.get(key).get());
    try {
      client.compareAndSet(key, Optional.empty(), value2);
      Assert.fail("compareAndSet should fail.");
    } catch (RawCASConflictException err) {
      Assert.assertEquals(value, err.getPrevValue().get());
    }
  }

  @Test
  public void rawPutIfAbsentTest() {
    long duration = 10;
    TimeUnit timeUnit = TimeUnit.SECONDS;
    ByteString key = ByteString.copyFromUtf8("key_atomic");
    ByteString value = ByteString.copyFromUtf8("value");
    ByteString value2 = ByteString.copyFromUtf8("value2");
    client.delete(key);
    Optional<ByteString> res1 = client.putIfAbsent(key, value, duration, timeUnit);
    assertFalse(res1.isPresent());
    Optional<ByteString> res2 = client.putIfAbsent(key, value2, duration, timeUnit);
    assertEquals(res2.get(), value);
    try {
      Thread.sleep(duration * 1000 + 100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    Optional<ByteString> res3 = client.putIfAbsent(key, value, duration, timeUnit);
    assertFalse(res3.isPresent());
  }
}
