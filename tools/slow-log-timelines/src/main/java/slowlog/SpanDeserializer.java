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
