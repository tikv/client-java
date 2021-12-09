package org.tikv.raw;

import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.CircuitBreakerOpenException;

public class SmartRawKVClientTest {
  private boolean enable = true;
  private int windowInSeconds = 2;
  private int errorThresholdPercentage = 100;
  private int requestVolumeThreshold = 10;
  private int sleepWindowInSeconds = 1;
  private int attemptRequestCount = 10;

  private int sleepDelta = 100;

  private TiSession session;
  private SmartRawKVClient client;

  @Before
  public void setup() {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    conf.setCircuitBreakEnable(enable);
    conf.setCircuitBreakAvailabilityWindowInSeconds(windowInSeconds);
    conf.setCircuitBreakAvailabilityErrorThresholdPercentage(errorThresholdPercentage);
    conf.setCircuitBreakAvailabilityRequestVolumnThreshold(requestVolumeThreshold);
    conf.setCircuitBreakSleepWindowInSeconds(sleepWindowInSeconds);
    conf.setCircuitBreakAttemptRequestCount(attemptRequestCount);
    session = TiSession.create(conf);
    client = session.createSmartRawClient();
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void testCircuitBreaker() throws InterruptedException {
    // CLOSED => OPEN
    {
      for (int i = 1; i <= requestVolumeThreshold; i++) {
        error();
      }
      Thread.sleep(windowInSeconds * 1000 + sleepDelta);

      Exception error = null;
      try {
        client.get(ByteString.copyFromUtf8("key"));
        assertTrue(false);
      } catch (Exception e) {
        error = e;
      }
      assertTrue(error instanceof CircuitBreakerOpenException);
    }

    // OPEN => CLOSED
    {
      Thread.sleep(sleepWindowInSeconds * 1000);
      for (int i = 1; i <= attemptRequestCount; i++) {
        success();
      }
      client.get(ByteString.copyFromUtf8("key"));
    }
  }

  private void success() {
    client.get(ByteString.copyFromUtf8("key"));
  }

  private void error() {
    try {
      client.callWithCircuitBreaker("error", () -> 1 / 0);
    } catch (Exception ignored) {
    }
  }
}
