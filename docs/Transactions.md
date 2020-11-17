## How to use transactions in TiKV-Client

TiKV Java Client supports Pre-Write/Commit operations in order to support writing transactions into TiKV with relatively large sizes.

### Entrance
```java
org.tikv.txn.TxnClient
```

### API

```java
/**
* Write keys and values in a transaction, if an error occurs,
* the whole transaction would fail.
*
* @param keys list of keys
* @param values list of values
*/
public void transaction(List<ByteString> keys, List<ByteString> values)
```

The following gives an example of how a transaction can be written.

```java
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.txn.KVClient;
import org.tikv.txn.TxnClient;

import java.util.List;

public class Main {
  public static void main(String[] args) {
    TiConfiguration conf = TiConfiguration.createDefault(YOUR_PD_ADDRESS);
    TiSession session = TiSession.create(conf);
    TxnClient client = session.createTxnClient();
    List<ByteString> keys = ImmutableList.of(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("k2"));
    List<ByteString> values = ImmutableList.of(ByteString.copyFromUtf8("v1"), ByteString.copyFromUtf8("v2"));

    // write kvs in a transaction
    client.transaction(keys, values);

    KVClient kvClient = session.createKVClient();
    long ts = session.getTimestamp().getVersion();
    for (int i = 0; i < keys.size(); i++) {
      ByteString val = kvClient.get(keys.get(i), ts);
      // ...
    }
  }
}
```
