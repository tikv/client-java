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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.text.SimpleDateFormat;

public class SlowLogSpanImpl implements SlowLogSpan {
  private final String name;
  private long startMS;
  private long endMS;
  private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

  public SlowLogSpanImpl(String name) {
    this.name = name;
    this.startMS = 0;
    this.endMS = 0;
  }

  @Override
  public void start() {
    this.startMS = System.currentTimeMillis();
  }

  @Override
  public void end() {
    this.endMS = System.currentTimeMillis();
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
