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

package org.tikv.common.importer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseRawKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.util.Pair;
import org.tikv.raw.RawKVClient;
import org.tikv.util.TestUtils;

public class RawKVIngestTest extends BaseRawKVTest {
  private TiSession session;

  private static final int KEY_NUMBER = 16;
  private static final String KEY_PREFIX = "prefix_rawkv_ingest_test_";
  private static final int KEY_LENGTH = KEY_PREFIX.length() + 10;
  private static final int VALUE_LENGTH = 16;

  @Before
  public void setup() {
    TiConfiguration conf = createTiConfiguration();
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void rawKVIngestTest() {
    RawKVClient client = session.createRawClient();

    // gen test data
    List<Pair<ByteString, ByteString>> sortedList = new ArrayList<>();
    for (int i = 0; i < KEY_NUMBER; i++) {
      byte[] key = TestUtils.genRandomKey(KEY_PREFIX, KEY_LENGTH);
      byte[] value = TestUtils.genRandomValue(VALUE_LENGTH);
      sortedList.add(Pair.create(ByteString.copyFrom(key), ByteString.copyFrom(value)));
    }
    sortedList.sort(
        (o1, o2) -> {
          Key k1 = Key.toRawKey(o1.first.toByteArray());
          Key k2 = Key.toRawKey(o2.first.toByteArray());
          return k1.compareTo(k2);
        });

    // ingest
    client.ingest(sortedList);

    // assert
    for (Pair<ByteString, ByteString> pair : sortedList) {
      Optional<ByteString> v = client.get(pair.first);
      assertTrue(v.isPresent());
      assertEquals(v.get(), pair.second);
    }
  }

  @Test
  public void rawKVIngestTestWithTTL() throws InterruptedException {
    long ttl = 10;
    RawKVClient client = session.createRawClient();

    // gen test data
    List<Pair<ByteString, ByteString>> sortedList = new ArrayList<>();
    for (int i = 0; i < KEY_NUMBER; i++) {
      byte[] key = TestUtils.genRandomKey(KEY_PREFIX, KEY_LENGTH);
      byte[] value = TestUtils.genRandomValue(VALUE_LENGTH);
      sortedList.add(Pair.create(ByteString.copyFrom(key), ByteString.copyFrom(value)));
    }
    sortedList.sort(
        (o1, o2) -> {
          Key k1 = Key.toRawKey(o1.first.toByteArray());
          Key k2 = Key.toRawKey(o2.first.toByteArray());
          return k1.compareTo(k2);
        });

    // ingest
    client.ingest(sortedList, ttl);

    // assert
    for (Pair<ByteString, ByteString> pair : sortedList) {
      Optional<ByteString> v = client.get(pair.first);
      assertTrue(v.isPresent());
      assertEquals(v.get(), pair.second);
    }

    // sleep
    Thread.sleep(ttl * 2 * 1000);
    // assert
    for (Pair<ByteString, ByteString> pair : sortedList) {
      Optional<ByteString> v = client.get(pair.first);
      assertFalse(v.isPresent());
    }
  }
}
