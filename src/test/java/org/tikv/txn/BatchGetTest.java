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
import static org.junit.Assert.assertNotSame;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.exception.KeyException;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class BatchGetTest extends TXNTest {

  @Test
  public void BatchGetResolveLockTest() throws Exception {
    long lockTTL = 20000L;
    String key1 = "batchGetResolveLockTestKey1";
    String key2 = "batchGetResolveLockTestKey2";
    String val1 = "val1";
    String val2 = "val2";
    String val1_update = "val1_update";
    String val2_update = "val2_update";

    // put key1 and key2
    putKV(key1, val1);
    putKV(key2, val2);

    // run 2PC background
    new Thread(
            () -> {
              long startTS = session.getTimestamp().getVersion();
              try (TwoPhaseCommitter twoPhaseCommitter =
                  new TwoPhaseCommitter(session, startTS, lockTTL)) {
                byte[] primaryKey = key1.getBytes("UTF-8");
                byte[] secondary = key2.getBytes("UTF-8");
                // prewrite primary key
                twoPhaseCommitter.prewritePrimaryKey(
                    ConcreteBackOffer.newCustomBackOff(5000),
                    primaryKey,
                    val1_update.getBytes("UTF-8"));
                List<BytePairWrapper> pairs =
                    Arrays.asList(new BytePairWrapper(secondary, val2_update.getBytes("UTF-8")));
                // prewrite secondary key
                twoPhaseCommitter.prewriteSecondaryKeys(primaryKey, pairs.iterator(), 5000);

                // get commitTS
                long commitTS = session.getTimestamp().getVersion();
                Thread.sleep(5000);
                // commit primary key
                twoPhaseCommitter.commitPrimaryKey(
                    ConcreteBackOffer.newCustomBackOff(5000), primaryKey, commitTS);
                // commit secondary key
                List<ByteWrapper> keys = Arrays.asList(new ByteWrapper(secondary));
                twoPhaseCommitter.commitSecondaryKeys(keys.iterator(), commitTS, 5000);
              } catch (Exception e) {
                KeyException keyException = (KeyException) e.getCause().getCause();
                assertNotSame("", keyException.getKeyErr().getCommitTsExpired().toString());
              }
            })
        .start();

    // wait 2PC get commitTS
    Thread.sleep(2000);
    // batch get key1 and key2
    try (KVClient kvClient = session.createKVClient()) {
      long version = session.getTimestamp().getVersion();
      ByteString k1 = ByteString.copyFromUtf8(key1);
      ByteString k2 = ByteString.copyFromUtf8(key2);

      BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(5000);
      List<KvPair> kvPairs = kvClient.batchGet(backOffer, Arrays.asList(k1, k2), version);
      // Since TiKV v4.0.0 write locked key will not block read. it is supported by Min Commit
      // Timestamp
      assertEquals(ByteString.copyFromUtf8(val1), kvPairs.get(0).getValue());
      assertEquals(ByteString.copyFromUtf8(val2), kvPairs.get(1).getValue());
      System.out.println(kvPairs);
      // wait 2PC finish
      Thread.sleep(10000);
    }
  }
}
