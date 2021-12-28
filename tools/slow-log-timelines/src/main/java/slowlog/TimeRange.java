package slowlog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class TimeRange {

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

  protected final Date start;
  protected final Date end;
  protected final Duration duration;

  public Date getStart() {
    return start;
  }

  public String getStartString() {
    return DATE_FORMAT.format(start);
  }


  public Date getEnd() {
    return end;
  }

  public String getEndString() {
    return DATE_FORMAT.format(end);
  }

  public Duration getDuration() {
    return duration;
  }

  public static TimeRange fromJson(JsonElement jsonElement) {
    Gson gson = new GsonBuilder().registerTypeAdapter(TimeRange.class, new TimeRangeDeserializer())
        .create();
    return gson.fromJson(jsonElement, TimeRange.class);
  }

  public TimeRange(Date start, Date end) {
    this.start = start;
    this.end = end;
    this.duration = Duration.ofMillis(end.getTime() - start.getTime());
  }
}
