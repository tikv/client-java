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

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseTxnKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.util.Pair;
import org.tikv.txn.KVClient;
import org.tikv.util.TestUtils;

public class TxnKVIngestTest extends BaseTxnKVTest {
  private TiSession session;

  private static final int KEY_NUMBER = 16;
  private static final String KEY_PREFIX = "prefix_txn_ingest_test_";
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
  public void txnIngestTest() {
    KVClient client = session.createKVClient();

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
    long version = session.getTimestamp().getVersion();
    for (Pair<ByteString, ByteString> pair : sortedList) {
      ByteString key = pair.first;
      ByteString v = client.get(key, version);
      assertEquals(v, pair.second);
    }
  }
}
