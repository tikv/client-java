package org.tikv.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.RawCASConflictException;

public class CASTest {
  private RawKVClient client;
  private boolean initialized;
  private static final Logger logger = LoggerFactory.getLogger(RawKVClientTest.class);
  private TiSession session;

  @Before
  public void setup() {
    try {
      TiConfiguration conf = TiConfiguration.createRawDefault();
      conf.setEnableAtomicForCAS(true);
      session = TiSession.create(conf);
      initialized = false;
      if (client == null) {
        client = session.createRawClient();
      }
      initialized = true;
    } catch (Exception e) {
      logger.warn(
          "Cannot initialize raw client, please check whether TiKV is running. Test skipped.", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void rawCASTest() {
    if (!initialized) return;
    ByteString key = ByteString.copyFromUtf8("key_atomic");
    ByteString value = ByteString.copyFromUtf8("value");
    ByteString value2 = ByteString.copyFromUtf8("value2");
    client.delete(key);
    client.compareAndSet(key, Optional.empty(), value);
    Assert.assertEquals(value, client.get(key).get());
    try {
      client.compareAndSet(key, Optional.empty(), value2);
      Assert.fail("compareAndSet should fail.");
    } catch (RawCASConflictException err) {
      Assert.assertEquals(value, err.getPrevValue().get());
    }
  }

  @Test
  public void rawPutIfAbsentTest() {
    if (!initialized) return;
    long ttl = 10;
    ByteString key = ByteString.copyFromUtf8("key_atomic");
    ByteString value = ByteString.copyFromUtf8("value");
    ByteString value2 = ByteString.copyFromUtf8("value2");
    client.delete(key);
    Optional<ByteString> res1 = client.putIfAbsent(key, value, ttl);
    assertFalse(res1.isPresent());
    Optional<ByteString> res2 = client.putIfAbsent(key, value2, ttl);
    assertEquals(res2.get(), value);
    try {
      Thread.sleep(ttl * 1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    Optional<ByteString> res3 = client.putIfAbsent(key, value, ttl);
    assertFalse(res3.isPresent());
  }
}
