/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.log;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowLogImpl implements SlowLog {
  private static final Logger logger = LoggerFactory.getLogger(SlowLogImpl.class);

  private static final int MAX_SPAN_SIZE = 1024;

  private static final Random random = new Random();

  private final List<SlowLogSpan> slowLogSpans = new ArrayList<>();
  private Throwable error = null;

  private final long startMS;
  /**
   * use System.nanoTime() to calculate duration, cause System.currentTimeMillis() is not monotonic
   */
  private final long startNS;

  private final long slowThresholdMS;

  private final long traceId;

  private long durationMS;

  public SlowLogImpl(long slowThresholdMS) {
    this.startMS = System.currentTimeMillis();
    this.startNS = System.nanoTime();
    this.slowThresholdMS = slowThresholdMS;
    this.traceId = random.nextLong();
  }

  @Override
  public synchronized SlowLogSpan start(String name) {
    SlowLogSpan slowLogSpan = new SlowLogSpanImpl(name, startMS, startNS);
    if (slowLogSpans.size() < MAX_SPAN_SIZE) {
      slowLogSpans.add(slowLogSpan);
    }
    slowLogSpan.start();
    return slowLogSpan;
  }

  @Override
  public long getTraceId() {
    return traceId;
  }

  @Override
  public long getThresholdMS() {
    return slowThresholdMS;
  }

  @Override
  public void setError(Throwable err) {
    this.error = err;
  }

  @Override
  public void log() {
    if (error != null || timeExceeded()) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
      logger.warn(
          String.format(
              "A request spent %s ms. start=%s, end=%s, SlowLog:%s",
              durationMS,
              dateFormat.format(startMS),
              dateFormat.format(startMS + durationMS),
              getSlowLogJson().toString()));
    }
  }

  boolean timeExceeded() {
    long currentNS = System.nanoTime();
    durationMS = (currentNS - startNS) / 1_000_000;
    return slowThresholdMS >= 0 && durationMS > slowThresholdMS;
  }

  JsonObject getSlowLogJson() {
    JsonObject jsonObject = new JsonObject();

    if (error != null) {
      jsonObject.addProperty("error", error.getMessage());
    }

    jsonObject.addProperty("trace_id", toUnsignedBigInteger(traceId));

    JsonArray jsonArray = new JsonArray();
    for (SlowLogSpan slowLogSpan : slowLogSpans) {
      jsonArray.add(slowLogSpan.toJsonElement());
    }
    jsonObject.add("spans", jsonArray);

    return jsonObject;
  }

  static BigInteger toUnsignedBigInteger(long i) {
    if (i >= 0) {
      return BigInteger.valueOf(i);
    } else {
      long withoutSign = i & ~(1L << 63);

      return (BigInteger.valueOf(1)).shiftLeft(63).add(BigInteger.valueOf(withoutSign));
    }
  }
}
