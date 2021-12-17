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

import static org.tikv.common.log.SlowLogImpl.DATE_FORMAT;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SlowLogSpanImpl implements SlowLogSpan {
  private final String name;
  private final long requestStartNS;
  private final long requestStartMS;

  private long startMS;
  private long endMS;
  /**
   * use System.nanoTime() to calculate duration, cause System.currentTimeMillis() is not monotonic
   */
  private long startNS;

  private long endNS;

  public SlowLogSpanImpl(String name, long requestStartMS, long requestStartNS) {
    this.name = name;
    this.requestStartMS = requestStartMS;
    this.requestStartNS = requestStartNS;
    this.startMS = 0;
    this.startNS = 0;
    this.endMS = 0;
    this.endNS = 0;
  }

  @Override
  public void start() {
    startNS = System.nanoTime();
    startMS = requestStartMS + (startNS - requestStartNS) / 1_000_000;
  }

  @Override
  public void end() {
    endNS = System.nanoTime();
    endMS = startMS + (endNS - startNS) / 1_000_000;
  }

  @Override
  public JsonElement toJsonElement() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", name);
    jsonObject.addProperty("start", getStartString());
    jsonObject.addProperty("end", getEndString());
    jsonObject.addProperty("duration", getDurationString());

    return jsonObject;
  }

  private String getStartString() {
    if (startMS == 0) {
      return "N/A";
    }
    return DATE_FORMAT.format(startMS);
  }

  private String getEndString() {
    if (endMS == 0) {
      return "N/A";
    }
    return DATE_FORMAT.format(endMS);
  }

  private String getDurationString() {
    if (startMS == 0 || endMS == 0) {
      return "N/A";
    }
    return (endMS - startMS) + "ms";
  }
}
