package org.tikv.txn;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.RegionException;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb;

public class TXNTest {
  static final int DEFAULT_TTL = 10;
  private TiSession session;
  RegionStoreClient.RegionStoreClientBuilder builder;

  @Before
  public void setUp() {
    TiConfiguration conf = TiConfiguration.createDefault();
    try {
      session = TiSession.create(conf);
      this.builder = session.getRegionStoreClientBuilder();
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

  void putKV(String key, String value) {
    long startTS = session.getTimestamp().getVersion();
    long commitTS = session.getTimestamp().getVersion();
    putKV(key, value, startTS, commitTS);
  }

  void putKV(String key, String value, long startTS, long commitTS) {
    Kvrpcpb.Mutation m =
        Kvrpcpb.Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setOp(Kvrpcpb.Op.Put)
            .setValue(ByteString.copyFromUtf8(value))
            .build();

    boolean res = prewriteString(Collections.singletonList(m), startTS, key, DEFAULT_TTL);
    assertTrue(res);
    res = commitString(Collections.singletonList(key), startTS, commitTS);
    assertTrue(res);
  }

  boolean prewriteString(List<Kvrpcpb.Mutation> mutations, long startTS, String primary, long ttl) {
    return prewrite(mutations, startTS, ByteString.copyFromUtf8(primary), ttl);
  }

  boolean prewrite(List<Kvrpcpb.Mutation> mutations, long startTS, ByteString primary, long ttl) {
    if (mutations.size() == 0) return true;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);

    for (Kvrpcpb.Mutation m : mutations) {
      while (true) {
        try {
          TiRegion region = session.getRegionManager().getRegionByKey(m.getKey());
          RegionStoreClient client = builder.build(region);
          client.prewrite(backOffer, primary, Collections.singletonList(m), startTS, ttl, false);
          break;
        } catch (RegionException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        }
      }
    }
    return true;
  }

  boolean commitString(List<String> keys, long startTS, long commitTS) {
    return commit(
        keys.stream().map(ByteString::copyFromUtf8).collect(Collectors.toList()),
        startTS,
        commitTS);
  }

  boolean commit(List<ByteString> keys, long startTS, long commitTS) {
    if (keys.size() == 0) return true;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);

    for (ByteString byteStringK : keys) {
      while (true) {
        try {
          TiRegion tiRegion = session.getRegionManager().getRegionByKey(byteStringK);
          RegionStoreClient client = builder.build(tiRegion);
          client.commit(backOffer, Collections.singletonList(byteStringK), startTS, commitTS);
          break;
        } catch (RegionException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        }
      }
    }
    return true;
  }

  String genRandomKey(int strLength) {
    Random rnd = ThreadLocalRandom.current();
    String prefix = rnd.nextInt(2) % 2 == 0 ? "a-test-" : "z-test-";
    StringBuilder ret = new StringBuilder(prefix);
    for (int i = 0; i < strLength; i++) {
      boolean isChar = (rnd.nextInt(2) % 2 == 0);
      if (isChar) {
        int choice = rnd.nextInt(2) % 2 == 0 ? 65 : 97;
        ret.append((char) (choice + rnd.nextInt(26)));
      } else {
        ret.append(rnd.nextInt(10));
      }
    }
    return ret.toString();
  }
}
