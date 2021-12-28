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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.time.DateTimeException;
import java.util.Date;
import java.util.Objects;

public class TimeRangeDeserializer implements JsonDeserializer<TimeRange> {

  @Override
  public TimeRange deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    String startStr = jsonObject.get("start").getAsString();
    String endStr = jsonObject.get("end").getAsString();
    if (Objects.equals(endStr, "N/A")) {
      endStr = startStr;
    }

    try {
      Date start = TimeRange.DATE_FORMAT.parse(startStr);
      Date end = TimeRange.DATE_FORMAT.parse(endStr);
      return new TimeRange(start, end);
    } catch (ParseException | DateTimeException e) {
      throw new JsonParseException(e.getMessage());
    }
  }
}
