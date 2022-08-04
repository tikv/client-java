/*
 * Copyright 2022 TiKV Project Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseTxnKVTest;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.Snapshot;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class TwoPhaseCommitterTest extends BaseTxnKVTest {
  private static final int WRITE_BUFFER_SIZE = 32 * 1024;
  private static final int TXN_COMMIT_BATCH_SIZE = 768 * 1024;
  private static final long DEFAULT_BATCH_WRITE_LOCK_TTL = 3600000;
  private RegionStoreClientBuilder clientBuilder;
  private TiSession session;
  private TxnKVClient txnKVClient;
  private Long lockTTLSeconds = 20L;

  @Before
  public void setUp() {
    TiConfiguration conf = createTiConfiguration();
    try {
      session = TiSession.create(conf);
      clientBuilder = session.getRegionStoreClientBuilder();
      txnKVClient = session.createTxnClient();
    } catch (Exception e) {
      fail("TiDB cluster may not be present");
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void autoClosableTest() throws Exception {
    long startTS = session.getTimestamp().getVersion();
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            WRITE_BUFFER_SIZE,
            new ThreadFactoryBuilder().setNameFormat("2pc-pool-%d").setDaemon(true).build());
    Assert.assertFalse(executorService.isShutdown());
    try (TwoPhaseCommitter twoPhaseCommitter =
        new TwoPhaseCommitter(
            session,
            startTS,
            DEFAULT_BATCH_WRITE_LOCK_TTL,
            TXN_COMMIT_BATCH_SIZE,
            TXN_COMMIT_BATCH_SIZE,
            WRITE_BUFFER_SIZE,
            1,
            true,
            3,
            executorService)) {}
    Assert.assertTrue(executorService.isShutdown());
  }

  @Test
  public void batchGetRetryTest() throws Exception {
    long startTS = session.getTimestamp().getVersion();

    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(60000);
    byte[] primaryKey = "key1".getBytes(StandardCharsets.UTF_8);
    byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    long version = session.getTimestamp().getVersion();
    ByteString bkey1 = ByteString.copyFromUtf8("key1");
    ByteString bkey2 = ByteString.copyFromUtf8("key2");
    ByteString bvalue1 = ByteString.copyFromUtf8("val1");
    ByteString bvalue2 = ByteString.copyFromUtf8("val2");
    ByteString bvalue3 = ByteString.copyFromUtf8("value3");
    List<Pair<ByteString, ByteString>> kvs =
        Arrays.asList(new Pair<>(bkey1, bvalue1), new Pair<>(bkey2, bvalue2));
    try (KVClient kvClient = session.createKVClient()) {
      new Thread(
              () -> {
                Snapshot snapshot = session.createSnapshot(session.getTimestamp());
                snapshot.batchGet(
                    60000, Collections.singletonList("hello".getBytes(Charset.defaultCharset())));
                try (TwoPhaseCommitter twoPhaseCommitter =
                    new TwoPhaseCommitter(session, startTS)) {
                  // first phrase: prewrite
                  twoPhaseCommitter.prewritePrimaryKey(
                      backOffer, primaryKey, "val1".getBytes(StandardCharsets.UTF_8));
                  List<BytePairWrapper> pairs = null;
                  pairs =
                      Collections.singletonList(
                          new BytePairWrapper(key2, "val2".getBytes(StandardCharsets.UTF_8)));
                  twoPhaseCommitter.prewriteSecondaryKeys(primaryKey, pairs.iterator(), 1000);
                  // second phrase: commit
                  long commitTS = session.getTimestamp().getVersion();

                  Thread.sleep(10000L);
                  twoPhaseCommitter.commitPrimaryKey(backOffer, primaryKey, commitTS);
                  List<ByteWrapper> keys = Collections.singletonList(new ByteWrapper(key2));
                  twoPhaseCommitter.commitSecondaryKeys(keys.iterator(), commitTS, 1000);

                  ByteString val = kvClient.get(bkey1, version);
                  assertEquals(bvalue1, val);

                  BackOffer cBackOffer = ConcreteBackOffer.newCustomBackOff(1000);
                  List<KvPair> kvPairs =
                      kvClient.batchGet(cBackOffer, Arrays.asList(bkey1, bkey2), version);
                  assertEquals(kvs.size(), kvPairs.size());
                  for (int i = 0; i < kvPairs.size(); i++) {
                    assertEquals(kvPairs.get(i).getKey(), kvs.get(i).first);
                    assertEquals(kvPairs.get(i).getValue(), kvs.get(i).second);
                  }
                  kvPairs = kvClient.scan(bkey1, ByteString.copyFromUtf8("key3"), version);
                  assertEquals(kvs.size(), kvPairs.size());
                  for (int i = 0; i < kvPairs.size(); i++) {
                    assertEquals(kvPairs.get(i).getKey(), kvs.get(i).first);
                    assertEquals(kvPairs.get(i).getValue(), kvs.get(i).second);
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })
          .start();

      Thread.sleep(1000L);

      try (TwoPhaseCommitter twoPhaseCommitter =
          new TwoPhaseCommitter(session, session.getTimestamp().getVersion())) {
        // first phrase: prewrite
        twoPhaseCommitter.prewritePrimaryKey(
            backOffer, primaryKey, "val3".getBytes(StandardCharsets.UTF_8));
        List<BytePairWrapper> pairs = null;
        pairs =
            Collections.singletonList(
                new BytePairWrapper(key2, "val2".getBytes(StandardCharsets.UTF_8)));
        twoPhaseCommitter.prewriteSecondaryKeys(primaryKey, pairs.iterator(), 1000);
        // second phrase: commit
        long commitTS = session.getTimestamp().getVersion();

        twoPhaseCommitter.commitPrimaryKey(backOffer, primaryKey, commitTS);
        List<ByteWrapper> keys = Collections.singletonList(new ByteWrapper(key2));
        twoPhaseCommitter.commitSecondaryKeys(keys.iterator(), commitTS, 1000);
      }
      // access
      List<Pair<ByteString, ByteString>> kvs2 =
          Arrays.asList(new Pair<>(bkey1, bvalue3), new Pair<>(bkey2, bvalue2));
      Snapshot snapshot = session.createSnapshot(session.getTimestamp());
      BackOffer cBackOffer = ConcreteBackOffer.newCustomBackOff(3000);
      List<KvPair> kvPairs =
          kvClient.batchGet(cBackOffer, Arrays.asList(bkey1, bkey2), snapshot.getVersion());
      assertEquals(kvs2.size(), kvPairs.size());
      for (int i = 0; i < kvPairs.size(); i++) {
        assertEquals(kvPairs.get(i).getKey(), kvs2.get(i).first);
        assertEquals(kvPairs.get(i).getValue(), kvs2.get(i).second);
      }
    }
  }
}
