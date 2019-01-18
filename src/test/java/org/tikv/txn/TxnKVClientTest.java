package org.tikv.txn;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;

import java.security.SecureRandom;
import java.util.List;
import java.util.function.Function;

public class TxnKVClientTest {

    TxnKVClient txnClient;

    @Before
    public void setClient() {
        try {
            txnClient = TxnKVClient.createClient("10.255.0.183:2479,10.255.0.105:2479,10.255.0.178:2479");
        } catch (Exception e) {
            System.out.println("Cannot initialize txn client. Test skipped.");
        }
    }

    @Test
    public void testScan() {
        byte[] startKey = ByteString.copyFromUtf8("test").toByteArray();
        List<Pair<byte[], byte[]>> result = txnClient.scan(startKey, 10);
        for(Pair<byte[], byte[]> item : result) {
            String key = new String(item.first);
            String value = new String(item.second);
            System.out.println("Key=" + key + ", Value=" + value);
        }
    }

    @Test
    public void testGet(){
        String key = "txn_test_set_0001";
        byte[] rawValue = txnClient.get(key.getBytes());
        String value = new String(rawValue);
        System.out.println("Value=" + value);
    }

    @Test
    public void testPut() {
        String key = "test_AAAAGAaJwbZnjgaPvypwZTiuMBFirzPf";
        String value = "put_value";
        boolean result = txnClient.put(key.getBytes(), value.getBytes());
        System.out.println("Put result=" + result);
    }

    @Test
    public void testTxnCommitSuccess() {
        String key = "test_AAAAGAaJwbZnjgaPvypwZTiuMBFirzPf";
        String value = "put_value 2222";
        ITransaction txn = this.txnClient.begin();
        try {
            txn.set(key.getBytes(), value.getBytes());
            txn.set("txn_test_set_0001".getBytes(), value.getBytes());
            boolean result = txn.commit();
            System.out.println("commit result=" + result);
        } catch (Exception e) {
            txn.rollback();
        }
    }

    @Test
    public void testTxnCommitWithConflict() {
        ByteString key = ByteString.copyFromUtf8("test_AAAAGAaJwbZnjgaPvypwZTiuMBFirzPf_primary_001");
        ByteString value = ByteString.copyFromUtf8("put_value primary");
        ByteString key2 = ByteString.copyFromUtf8("txn_test_secondary_0001");
        ByteString value2 = ByteString.copyFromUtf8("put_value secondary");
        long startVersion = txnClient.getTimestamp().getVersion();
        ITransaction txn = this.txnClient.begin();
        txn.set(key.toByteArray(), value.toByteArray());
        txn.set(key2.toByteArray(), value2.toByteArray());
        try {
            Kvrpcpb.Mutation mutation = Kvrpcpb.Mutation.newBuilder()
                    .setKey(key)
                    .setValue(value)
                    .setOp(Kvrpcpb.Op.Put)
                    .build();
            System.out.println("startVersion=" + startVersion);
            TiRegion region = txnClient.getSession().getRegionManager().getRegionByKey(key);
            txnClient.prewrite(ConcreteBackOffer.newCustomBackOff(2000), Lists.newArrayList(mutation), key.toByteArray(),
                    1000, startVersion, region.getId());
            boolean result = txn.commit();
            System.out.println("commit result=" + result);
        } catch (Exception e) {
            txn.rollback();
        }
    }

    /**
     * func BackOff(attempts uint) int {
     * 	upper := int(math.Min(float64(retryBackOffCap), float64(retryBackOffBase)*math.Pow(2.0, float64(attempts))))
     * 	sleep := time.Duration(rand.Intn(upper)) * time.Millisecond
     * 	time.Sleep(sleep)
     * 	return int(sleep)
     * }
     */
    int retryBackOffCap = 100;
    int retryBackOffBase = 1;
    int maxRetryCnt = 100;
    SecureRandom random = new SecureRandom();

    private int backoff(int attempts) {
        int upper = (int)(Math.min(retryBackOffCap, retryBackOffBase * Math.pow(2.0, attempts)));
        int sleep = random.nextInt(upper);
        try {
            Thread.sleep(sleep);
            System.out.println("sleep " + sleep + " at attempts " + attempts);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sleep;
    }

    /**
     *
     * @param function
     * @return
     */
    private boolean runTxnWithRetry(Function<ITransaction, Boolean> function) {
        for(int i=0 ; i < maxRetryCnt; i++) {
            ITransaction txn = txnClient.begin();
            Boolean result = function.apply(txn);
            if(!result) {
                txn.rollback();
                continue;
            }
            boolean commit = txn.commit();
            if(commit) {
                return true;
            }
            backoff(i);
        }
        return false;
    }

    @Test
    public void testCommit_NoConflict_Success() {
        ByteString key = ByteString.copyFromUtf8("test_AAAAGAaJwbZnjgaPvypwZTiuMBFirzPf_primary_001");
        System.out.println("old value=" + new String(txnClient.get(key.toByteArray())));

        boolean commit1 = runTxnWithRetry((txn) -> {
            byte[] txn1Value = txn.get(key.toByteArray());
            System.out.println("txn1 start ts=" + txn.getStartTS());
            long txn1NewValue = Long.valueOf(new String(txn1Value)) + 1;
            System.out.println("txn1 new value=" + txn1NewValue);
            txn.set(key.toByteArray(), ByteString.copyFromUtf8(txn1NewValue + "").toByteArray());
            //txn.lockKeys(Key.toRawKey(key.toByteArray()));
            return true;
        });
        boolean commit2 = runTxnWithRetry((txn) -> {
            byte[] txn2Value = txn.get(key.toByteArray());
            System.out.println("txn2 start ts=" + txn.getStartTS());
            long txn2NewValue = Long.valueOf(new String(txn2Value)) + 1;
            System.out.println("txn2 new value=" + txn2NewValue);
            txn.set(key.toByteArray(), ByteString.copyFromUtf8(txn2NewValue + "").toByteArray());
            return true;
        });

        System.out.println("commit result1=" + commit1);
        System.out.println("commit result2=" + commit2);

        System.out.println("new value=" + new String(txnClient.get(key.toByteArray())));
    }

    @Test
    public void testCommit_Retry() {
        ByteString key = ByteString.copyFromUtf8("test_AAAAGAaJwbZnjgaPvypwZTiuMBFirzPf_primary_001");
        System.out.println("old value=" + new String(txnClient.get(key.toByteArray())));

        ITransaction txn1 = txnClient.begin((txn) -> {
            byte[] txn1Value = txn.get(key.toByteArray());
            System.out.println("txn1 start ts=" + txn.getStartTS());
            long txn1NewValue = Long.valueOf(new String(txn1Value)) + 1;
            System.out.println("txn1 new value=" + txn1NewValue);
            txn.set(key.toByteArray(), ByteString.copyFromUtf8(txn1NewValue + "").toByteArray());
            return true;
        });

        ITransaction txn2 = txnClient.begin((txn) -> {
            byte[] txn2Value = txn.get(key.toByteArray());
            System.out.println("txn2 start ts=" + txn.getStartTS());
            long txn2NewValue = Long.valueOf(new String(txn2Value)) + 1;
            System.out.println("txn2 new value=" + txn2NewValue);
            txn.set(key.toByteArray(), ByteString.copyFromUtf8(txn2NewValue + "").toByteArray());
            return true;
        });
        boolean commit1 = txn1.commit();
        boolean commit2 = txn2.commit();

        System.out.println("commit result1=" + commit1);
        System.out.println("commit result2=" + commit2);

        System.out.println("new value=" + new String(txnClient.get(key.toByteArray())));
    }

    @Test
    public void testCommit_Conflict_RetrySuccess() {
        ByteString key = ByteString.copyFromUtf8("test_AAAAGAaJwbZnjgaPvypwZTiuMBFirzPf_primary_001");
        System.out.println("old value=" + new String(txnClient.get(key.toByteArray())));

        ITransaction txn1 = txnClient.begin((txn) -> {
            byte[] txn1Value = txn.get(key.toByteArray());
            System.out.println("txn1 start ts=" + txn.getStartTS());
            long txn1NewValue = Long.valueOf(new String(txn1Value)) + 1;
            System.out.println("txn1 new value=" + txn1NewValue);
            txn.set(key.toByteArray(), ByteString.copyFromUtf8(txn1NewValue + "").toByteArray());
            return true;
        });

        ITransaction txn2 = txnClient.begin((txn) -> {
            byte[] txn2Value = txn.get(key.toByteArray());
            System.out.println("txn2 start ts=" + txn.getStartTS());
            long txn2NewValue = Long.valueOf(new String(txn2Value)) + 1;
            System.out.println("txn2 new value=" + txn2NewValue);
            txn.set(key.toByteArray(), ByteString.copyFromUtf8(txn2NewValue + "").toByteArray());
            return true;
        });
        boolean commit2 = txn2.commit();
        boolean commit1 = txn1.commit();

        System.out.println("commit result1=" + commit1);
        System.out.println("commit result2=" + commit2);

        System.out.println("new value=" + new String(txnClient.get(key.toByteArray())));
    }

    @Test
    public void testCommit_Fail() {
        ByteString key = ByteString.copyFromUtf8("test_AAAAGAaJwbZnjgaPvypwZTiuMBFirzPf_primary_001");
        System.out.println("old value=" + new String(txnClient.get(key.toByteArray())));

        ITransaction txn1 = txnClient.begin();
        byte[] txn2Value = txn1.get(key.toByteArray());
        System.out.println("txn2 start ts=" + txn1.getStartTS());
        long txn2NewValue = Long.valueOf(new String(txn2Value)) + 1;
        System.out.println("txn2 new value=" + txn2NewValue);
        txn1.set(key.toByteArray(), ByteString.copyFromUtf8(txn2NewValue + "").toByteArray());
        boolean commit1 = runTxnWithRetry((txn) -> {
            byte[] txn1Value = txn.get(key.toByteArray());
            System.out.println("txn1 start ts=" + txn.getStartTS());
            long txn1NewValue = Long.valueOf(new String(txn1Value)) + 1;
            System.out.println("txn1 new value=" + txn1NewValue);
            txn.set(key.toByteArray(), ByteString.copyFromUtf8(txn1NewValue + "").toByteArray());
            return true;
        });

        boolean commit2 = txn1.commit();
        System.out.println("commit result1=" + commit1);
        System.out.println("commit result2=" + commit2);

        System.out.println("new value=" + new String(txnClient.get(key.toByteArray())));
    }
}
