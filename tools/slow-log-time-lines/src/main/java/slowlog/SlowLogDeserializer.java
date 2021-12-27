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

