package org.tikv.common;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration.ApiVersion;
import org.tikv.raw.RawKVClient;

public class ApiVersionTest {
  private static final Logger logger = LoggerFactory.getLogger(ApiVersionTest.class);

  private TiConfiguration createConfiguration() {
    TiConfiguration conf = TiConfiguration.createRawDefault();

    conf.setTest(true);
    conf.setEnableAtomicForCAS(true);
    conf.setWarmUpEnable(false);

    return conf;
  }

  private RawKVClient createRawClient(ApiVersion apiVersion) {
    TiConfiguration conf = createConfiguration();
    conf.setApiVersion(apiVersion);
    return TiSession.create(conf).createRawClient();
  }

  private ApiVersion getClusterApiVersion() {
    return StoreConfig.acquireApiVersion(TiSession.create(createConfiguration()).getPDClient());
  }

  private boolean getClusterEnabledTtl() {
    return StoreConfig.ifTllEnable(TiSession.create(createConfiguration()).getPDClient());
  }

  private boolean minTiKVVersion(String version) {
    return StoreVersion.minTiKVVersion(
        version, TiSession.create(createConfiguration()).getPDClient());
  }

  @Test
  public void testAccessV2Cluster() {
    Assume.assumeTrue(getClusterApiVersion().isV2());

    Assert.assertTrue(getClusterEnabledTtl());

    // V1 client can't access V2 cluster
    RawKVClient client = createRawClient(ApiVersion.V1);
    try {
      client.get(ByteString.EMPTY);
      Assert.fail("Should not be able to access V2 cluster with V1 client");
    } catch (Exception e) {
      Assert.assertNotNull(e);
    }

    try {
      client.put(ByteString.EMPTY, ByteString.EMPTY, 10);
      Assert.fail("Should not be able to access V2 cluster with V1 client using TTL");
    } catch (Exception e) {
      Assert.assertNotNull(e);
    }

    // V2 client can access V2 cluster
    client = createRawClient(ApiVersion.V2);
    client.putIfAbsent(ByteString.EMPTY, ByteString.EMPTY);
    client.put(ByteString.EMPTY, ByteString.EMPTY, 10);
    Optional<ByteString> result = client.get(ByteString.EMPTY);
    Assert.assertTrue(result.isPresent());
    result.ifPresent(value -> Assert.assertEquals(ByteString.EMPTY, value));

    client.delete(ByteString.EMPTY);
  }

  @Test
  public void testAccessV1Cluster() {
    Assume.assumeTrue(minTiKVVersion("6.0.0"));
    Assume.assumeTrue(getClusterApiVersion().isV1());
    Assume.assumeFalse(getClusterEnabledTtl());

    // V1 client can access V1 cluster's raw data, no ttl allowed
    RawKVClient client = createRawClient(ApiVersion.V1);
    client.put(ByteString.EMPTY, ByteString.EMPTY);
    Optional<ByteString> result = client.get(ByteString.EMPTY);
    Assert.assertTrue(result.isPresent());
    result.ifPresent(value -> Assert.assertEquals(ByteString.EMPTY, value));
    client.delete(ByteString.EMPTY);

    try {
      client.put(ByteString.EMPTY, ByteString.EMPTY, 10);
      Assert.fail("Should not be able to access V1 cluster without TTL");
    } catch (Exception e) {
      Assert.assertNotNull(e);
    }

    // V2 client can't access V1 cluster
    client = createRawClient(ApiVersion.V2);
    try {
      client.put(ByteString.EMPTY, ByteString.EMPTY);
      Assert.fail("Should not be able to access V1 cluster with V2 Client");
    } catch (Exception e) {
      Assert.assertNotNull(e);
    }
  }

  @Test
  public void testAccessV1ClusterWithTtl() throws InterruptedException {
    Assume.assumeTrue(minTiKVVersion("6.0.0"));
    Assume.assumeTrue(getClusterApiVersion().isV1());
    Assume.assumeTrue(getClusterEnabledTtl());

    // V1 client can access V1 cluster's raw data, ttl allowed
    RawKVClient client = createRawClient(ApiVersion.V1);
    client.put(ByteString.EMPTY, ByteString.EMPTY, 5);
    Optional<ByteString> result = client.get(ByteString.EMPTY);
    Assert.assertTrue(result.isPresent());
    result.ifPresent(value -> Assert.assertEquals(ByteString.EMPTY, value));

    logger.info("Waiting for ttl to expire");
    Thread.sleep(5000);

    Assert.assertFalse(client.get(ByteString.EMPTY).isPresent());

    // V2 client can't access V1 cluster with TTL
    client = createRawClient(ApiVersion.V2);
    try {
      client.put(ByteString.EMPTY, ByteString.EMPTY, 5);
      Assert.fail("Should not be able to access V1 cluster with TTL with V2 Client");
    } catch (Exception e) {
      Assert.assertNotNull(e);
    }
  }
}
