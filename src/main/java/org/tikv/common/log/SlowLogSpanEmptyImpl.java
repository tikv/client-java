/*
 *
 * Copyright 2021 TiKV Project Authors.
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

public class SlowLogSpanEmptyImpl implements SlowLogSpan {

  public static final SlowLogSpanEmptyImpl INSTANCE = new SlowLogSpanEmptyImpl();

  private SlowLogSpanEmptyImpl() {}

  @Override
  public void start() {}

  @Override
  public void end() {}

  @Override
  public JsonElement toJsonElement() {
    return new JsonObject();
  }
}
