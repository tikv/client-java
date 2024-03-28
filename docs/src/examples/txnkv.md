# TxnKV

Below is the basic usages of TxnKV.
Data should be written into TxnKV using [`TwoPhaseCommitter`], and be read using [`org.tikv.txn.KVClient`][KVClient].

[`TwoPhaseCommitter`]: https://tikv.github.io/client-java/apidocs/org/tikv/txn/TwoPhaseCommitter.html
[KVClient]: https://tikv.github.io/client-java/apidocs/org/tikv/txn/KVClient.html

```java
import java.util.Arrays;
import java.util.List;

import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;
import org.tikv.txn.TwoPhaseCommitter;

public class App {
    public static void main(String[] args) throws Exception {
        TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
        try (TiSession session = TiSession.create(conf)) {
            // two-phrase write
            long startTS = session.getTimestamp().getVersion();
            try (TwoPhaseCommitter twoPhaseCommitter = new TwoPhaseCommitter(session, startTS)) {
                BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);
                byte[] primaryKey = "key1".getBytes("UTF-8");
                byte[] key2 = "key2".getBytes("UTF-8");

                // first phrase: prewrite
                twoPhaseCommitter.prewritePrimaryKey(backOffer, primaryKey, "val1".getBytes("UTF-8"));
                List<BytePairWrapper> pairs = Arrays
                        .asList(new BytePairWrapper(key2, "val2".getBytes("UTF-8")));
                twoPhaseCommitter.prewriteSecondaryKeys(primaryKey, pairs.iterator(), 1000);

                // second phrase: commit
                long commitTS = session.getTimestamp().getVersion();
                twoPhaseCommitter.commitPrimaryKey(backOffer, primaryKey, commitTS);
                List<ByteWrapper> keys = Arrays.asList(new ByteWrapper(key2));
                twoPhaseCommitter.commitSecondaryKeys(keys.iterator(), commitTS, 1000);
            }

            try (KVClient kvClient = session.createKVClient()) {
                long version = session.getTimestamp().getVersion();
                ByteString key1 = ByteString.copyFromUtf8("key1");
                ByteString key2 = ByteString.copyFromUtf8("key2");

                // get value of a single key
                ByteString val = kvClient.get(key1, version);
                System.out.println(val);

                // get value of multiple keys
                BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);
                List<KvPair> kvPairs = kvClient.batchGet(backOffer, Arrays.asList(key1, key2), version);
                System.out.println(kvPairs);

                // get value of a range of keys
                kvPairs = kvClient.scan(key1, ByteString.copyFromUtf8("key3"), version);
                System.out.println(kvPairs);
            }
        }
    }
}
```