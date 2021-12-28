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

package slowlog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;

public class SpanDeserializer implements JsonDeserializer<Span> {

  private String name;

  @Override
  public Span deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

    Gson gson = new GsonBuilder()
        .registerTypeAdapter(TimeRange.class, new TimeRangeDeserializer())
        .create();
    SpanDeserializer spanDeserializer = gson.fromJson(jsonElement, SpanDeserializer.class);
    TimeRange timeRange = gson.fromJson(jsonElement, TimeRange.class);
    return new Span(spanDeserializer.name, timeRange);
  }
}
