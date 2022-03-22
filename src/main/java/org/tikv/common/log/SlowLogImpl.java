/*
 *
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.log;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowLogImpl implements SlowLog {

  private static final Logger logger = LoggerFactory.getLogger(SlowLogImpl.class);

  private static final int MAX_SPAN_SIZE = 1024;

  private final List<SlowLogSpan> slowLogSpans = new ArrayList<>();
  private final HashMap<String, Object> fields = new HashMap<>();
  private Throwable error = null;

  private final long startMS;
  /**
   * use System.nanoTime() to calculate duration, cause System.currentTimeMillis() is not monotonic
   */
  private final long startNS;

  private final long slowThresholdMS;

  /** Key-Value pairs which will be logged, e.g. function name, key, region, etc. */
  private final Map<String, Object> properties;

  public SlowLogImpl(long slowThresholdMS, Map<String, Object> properties) {
    this.startMS = System.currentTimeMillis();
    this.startNS = System.nanoTime();
    this.slowThresholdMS = slowThresholdMS;
    this.properties = new HashMap<>(properties);
  }

  public SlowLogImpl(long slowThresholdMS) {
    this(slowThresholdMS, new HashMap<>());
  }

  @Override
  public void addProperty(String key, String value) {
    this.properties.put(key, value);
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
  public void setError(Throwable err) {
    this.error = err;
  }

  @Override
  public void log() {
    long currentNS = System.nanoTime();
    long currentMS = startMS + (currentNS - startNS) / 1_000_000;
    if (error != null || (slowThresholdMS >= 0 && currentMS - startMS > slowThresholdMS)) {
      logger.warn("SlowLog:" + getSlowLogString(currentMS));
    }
  }

  private String getSlowLogString(long currentMS) {
    SimpleDateFormat dateFormat = getSimpleDateFormat();
    JsonObject jsonObject = new JsonObject();

    jsonObject.addProperty("start", dateFormat.format(startMS));
    jsonObject.addProperty("end", dateFormat.format(currentMS));
    jsonObject.addProperty("duration", (currentMS - startMS) + "ms");
    if (error != null) {
      jsonObject.addProperty("error", error.getMessage());
    }

    for (Entry<String, Object> entry : properties.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof List) {
        JsonArray field = new JsonArray();
        for (Object o : (List<?>) value) {
          field.add(o.toString());
        }
        jsonObject.add(entry.getKey(), field);
      } else if (value instanceof Map) {
        JsonObject field = new JsonObject();
        for (Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
          field.addProperty(e.getKey().toString(), e.getValue().toString());
        }
        jsonObject.add(entry.getKey(), field);
      } else {
        jsonObject.addProperty(entry.getKey(), value.toString());
      }
    }

    JsonArray jsonArray = new JsonArray();
    for (SlowLogSpan slowLogSpan : slowLogSpans) {
      jsonArray.add(slowLogSpan.toJsonElement());
    }
    jsonObject.add("spans", jsonArray);

    return jsonObject.toString();
  }

  public static SimpleDateFormat getSimpleDateFormat() {
    return new SimpleDateFormat("HH:mm:ss.SSS");
  }
}
