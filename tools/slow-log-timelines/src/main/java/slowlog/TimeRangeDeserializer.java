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
