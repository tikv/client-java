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