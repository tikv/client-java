# TxnKV

Below is the basic usages of TxnKV.

```java
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb;

public class Main {
  public static void main() {
    TiConfiguration conf = TiConfiguration.createDefault();
    try (TiSession session = TiSession.create(conf)) {
      ByteString key1 = ByteString.copyFromUtf8("key1");
      ByteString key2 = ByteString.copyFromUtf8("key2");
      ArrayList<Kvrpcpb.Mutation> mutations =
          new ArrayList<Kvrpcpb.Mutation>() {
            {
              add(
                  Kvrpcpb.Mutation.newBuilder()
                      .setKey(key1)
                      .setOp(Kvrpcpb.Op.Put)
                      .setValue(ByteString.copyFromUtf8("value"))
                      .build());
              add(
                  Kvrpcpb.Mutation.newBuilder()
                      .setKey(key2)
                      .setOp(Kvrpcpb.Op.Put)
                      .setValue(ByteString.copyFromUtf8("value"))
                      .build());
            }
          };
      BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);
      long startTS = session.getTimestamp().getVersion();
      long commitTS = session.getTimestamp().getVersion();
      ArrayList<ByteString> commitKeys =
          new ArrayList<ByteString>() {
            {
              add(key1);
              add(key2);
            }
          };
      TiRegion region = session.getRegionManager().getRegionByKey(key1);
      try (RegionStoreClient client = session.getRegionStoreClientBuilder().build(region)) {
        client.prewrite(backOffer, key1, mutations, 10, startTS, false);
        client.commit(backOffer, commitKeys, startTS, commitTS);
      }
    }
  }
}
```