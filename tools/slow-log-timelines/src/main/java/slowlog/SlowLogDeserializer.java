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
import com.google.gson.annotations.SerializedName;
import java.lang.reflect.Type;

public class SlowLogDeserializer implements JsonDeserializer<SlowLog> {

  @SerializedName("func")
  private String name;

  private String region;
  private String key;
  private Span[] spans;


  @Override
  public SlowLog deserialize(
      JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext)
      throws JsonParseException {

    Gson gson = new GsonBuilder()
        .registerTypeAdapter(Span.class, new SpanDeserializer())
        .registerTypeAdapter(TimeRange.class, new TimeRangeDeserializer())
        .create();
    TimeRange timeRange = TimeRange.fromJson(jsonElement);
    SlowLogDeserializer deserializer = gson.fromJson(jsonElement, SlowLogDeserializer.class);
    return new SlowLog(deserializer.name, timeRange, deserializer.region, deserializer.key,
        deserializer.spans);
  }
}
