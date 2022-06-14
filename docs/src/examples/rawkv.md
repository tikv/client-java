# RawKV

Below is the basic usages of RawKV. See [API document] to see a full list of methods available.

[API document]: https://tikv.github.io/client-java/apidocs/org/tikv/raw/RawKVClient

```java
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

public class Main {
  public static void main() {
    // You MUST create a raw configuration if you are using RawKVClient.
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    RawKVClient client = session.createRawClient();

    // put
    client.put(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("Hello"));
    client.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8(","));
    client.put(ByteString.copyFromUtf8("k3"), ByteString.copyFromUtf8("World"));
    client.put(ByteString.copyFromUtf8("k4"), ByteString.copyFromUtf8("!"));
    client.put(ByteString.copyFromUtf8("k5"), ByteString.copyFromUtf8("Raw KV"));

    // get
    Optional<ByteString> result = client.get(ByteString.copyFromUtf8("k1"));
    System.out.println(result.get().toStringUtf8());

    // batch get
    List<Kvrpcpb.KvPair> list = client.batchGet(new ArrayList<ByteString>() {{
        add(ByteString.copyFromUtf8("k1"));
        add(ByteString.copyFromUtf8("k3"));
    }});
    System.out.println(list);

    // scan
    list = client.scan(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("k6"), 10);
    System.out.println(list);

    // close
    client.close();
    session.close();
  }
}
```


## API V2
With TiKV version >= 6.1.0, we release a new feature that allows the coexistence of transcation data and raw data. This feature could allow the [TiCDC](https://github.com/tikv/migration/tree/main/cdc) to capture the change of the data, and these events could be consumed by the downstream infrastructure like Kafka or another TiKV cluster for backup.

To enable the API V2 mode, users need to specify the API version of the client.

```java
// import ...
import org.tikv.common.TiConfiguration.ApiVersion;

public class Main {
  public static void main() {
    TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
    conf.setApiVersion(ApiVersion.V2);
    try(TiSession session = TiSession.create(conf)) {
      try(RawKVClient client = session.createRawClient()) {
        // The client will read and write date in the format of API V2, which is
        // transparent to the users.
        client.put(ByteString.copyFromUtf8("hello"), ByteString.copyFromUtf8("world"));
        // other client operations.
      }
    }
  }
}
```

### Compatibility

The V2 Client should not access the cluster other than V2, this requires users to enabel the API V2 for the cluster:

```toml
[storage]
# The V2 cluster must enable ttl for RawKV explicitly
enable-ttl = true
api-version = 2
```

If V2 client accesses a V1 cluster or V1 cluster accesses a V2 cluster, the requests will be denyed by the cluster. You can check the compatibility via the following matrix.


|                       | V1 Server | V1TTL Server | V2 Server |
| --------------------- | --------- | ------------ | --------- |
| V1 RawClient          | Raw       | Raw          | Error     |
| V1 RawClient with TTL | Error     | Raw          | Error     |
| V1 TxnClient          | Txn       | Error        | Error     |
| V1 TiDB               | TiDB Data | Error        | TiDB Data |
| V2 RawClient          | Error     | Error        | Raw       |
| V2 TxnClient          | Error     | Error        | Txn       |