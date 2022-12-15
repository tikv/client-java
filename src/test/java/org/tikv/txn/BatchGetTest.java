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
    long startTS = session.getTimestamp().getVersion();
    String key1 = "batchGetResolveLockTestKey1";
    String key2 = "batchGetResolveLockTestKey2";
    String val1 = "val1";
    String val2 = "val2";

    new Thread(
            () -> {
              try (TwoPhaseCommitter twoPhaseCommitter =
                  new TwoPhaseCommitter(session, startTS, lockTTL)) {
                byte[] primaryKey = key1.getBytes("UTF-8");
                byte[] secondary = key2.getBytes("UTF-8");
                // prewrite primary key
                twoPhaseCommitter.prewritePrimaryKey(
                    ConcreteBackOffer.newCustomBackOff(5000), primaryKey, val1.getBytes("UTF-8"));
                List<BytePairWrapper> pairs =
                    Arrays.asList(new BytePairWrapper(secondary, val2.getBytes("UTF-8")));
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
    }

    // wait 2PC commit finish
    Thread.sleep(10000);
  }
}
