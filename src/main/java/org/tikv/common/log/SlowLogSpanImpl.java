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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class SlowLogSpanImpl implements SlowLogSpan {
  private final String name;
  private final long requestStartInstantNS;
  private final long requestStartUnixNS;

  /** Key-Value pairs which will be logged, e.g. function name, key, region, etc. */
  private final Map<String, String> properties;

  /**
   * use System.nanoTime() to calculate duration, cause System.currentTimeMillis() is not monotonic
   */
  private long startInstantNS;

  private long endInstantNS;

  public SlowLogSpanImpl(String name, long requestStartMS, long requestStartInstantNS) {
    this.name = name;
    this.requestStartUnixNS = requestStartMS * 1_000_000;
    this.requestStartInstantNS = requestStartInstantNS;
    this.properties = new HashMap<>();
    this.startInstantNS = 0;
    this.endInstantNS = 0;
  }

  @Override
  public void addProperty(String key, String value) {
    properties.put(key, value);
  }

  @Override
  public void start() {
    startInstantNS = System.nanoTime();
  }

  @Override
  public void end() {
    endInstantNS = System.nanoTime();
  }

  @Override
  public JsonElement toJsonElement() {
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("event", name);
    jsonObject.addProperty("begin", dateFormat.format(getStartUnixNS() / 1_000_000));
    jsonObject.addProperty("begin_unix_ms", getStartUnixNS() / 1_000_000);
    jsonObject.addProperty("duration_ms", getDurationNS() / 1_000_000);

    if (!properties.isEmpty()) {
      JsonObject propertiesObject = new JsonObject();
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        propertiesObject.addProperty(entry.getKey(), entry.getValue());
      }
      jsonObject.add("properties", propertiesObject);
    }

    return jsonObject;
  }

  private long getStartUnixNS() {
    return requestStartUnixNS + (startInstantNS - requestStartInstantNS);
  }

  private long getDurationNS() {
    if (startInstantNS == 0 || endInstantNS == 0) {
      return -1;
    }
    return endInstantNS - startInstantNS;
  }
}
