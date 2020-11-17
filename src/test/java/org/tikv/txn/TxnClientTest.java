/*
 * Copyright 2020 PingCAP, Inc.
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

package org.tikv.txn;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.TiKVException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TxnClientTest {
  private static final String DEFAULT_PD_ADDRESS = "127.0.0.1:2379";
  private static final String TXN_PREFIX = "txn_\u0001_";
  private static final Logger logger = LoggerFactory.getLogger(TxnClientTest.class);
  private TxnClient client;
  private boolean initialized;
  private TiSession session;

  private static ByteString getKey(String key) {
    return ByteString.copyFromUtf8(TXN_PREFIX + key);
  }

  private static ByteString getValue(String value) {
    return ByteString.copyFromUtf8(value);
  }

  @Before
  public void setup() throws IOException {
    try {
      session = TiSession.create(TiConfiguration.createDefault(DEFAULT_PD_ADDRESS));
      initialized = false;
      if (client == null) {
        client = session.createTxnClient();
      }
      initialized = true;
    } catch (Exception e) {
      logger.warn("Cannot initialize raw client. Test skipped.", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void simpleTest() {
    if (!initialized) return;
    ByteString key1 = getKey("key1");
    ByteString key2 = getKey("key2");
    ByteString key3 = getKey("key3");
    ByteString value1 = getValue("value1");
    ByteString value2 = getValue("value2");
    ByteString value3 = getValue("value3");

    List<ByteString> keys = ImmutableList.of(key1, key2, key3);
    List<ByteString> values = ImmutableList.of(value1, value2, value3);

    try {
      // write transaction
      client.transaction(keys, values);

      KVClient kvClient = session.createKVClient();
      long ts = session.getTimestamp().getVersion();
      for (int i = 0; i < keys.size(); i++) {
        checkEquals(values.get(i), kvClient.get(keys.get(i), ts));
      }
    } catch (final TiKVException e) {
      logger.warn("Test fails with Exception: " + e);
    }
  }

  private boolean isEquals(ByteString expected, ByteString actual) {
    if (expected == null) {
      return actual == null;
    } else if (actual == null) {
      return false;
    } else {
      logger.info("expected=" + expected.toStringUtf8() + " actual=" + actual.toStringUtf8());
      return Arrays.equals(expected.toByteArray(), actual.toByteArray());
    }
  }

  private void checkEquals(ByteString expected, ByteString actual) {
    if (!isEquals(expected, actual)) {
      throw new RuntimeException("expected not equal to actual");
    }
  }
}
