package slowlog;

import com.diogonunes.jcolor.Attribute;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import java.time.Duration;
import com.diogonunes.jcolor.Ansi;
import org.apache.commons.lang3.StringUtils;

public class SlowLog {

  private int resolution = 80;
  private Duration warningThreshold = Duration.ofMillis(10);
  private final String name;
  private final TimeRange range;
  private final String region;
  private final String key;
  private final Span[] spans;

  public static SlowLog fromJson(JsonReader reader) throws JsonParseException {
    Gson gson = new GsonBuilder().registerTypeAdapter(SlowLog.class, new SlowLogDeserializer())
        .create();
    return gson.fromJson(reader, SlowLog.class);
  }

  public void dump() {
    float factor = (float) resolution / (float) range.getDuration().toNanos();

    System.out.println(
        Ansi.colorize(String.format("function: %s", name), Attribute.BRIGHT_YELLOW_TEXT()));
    System.out.println(Ansi.colorize(String.format("start: %s", range.getStartString()),
        Attribute.BRIGHT_YELLOW_TEXT()));
    System.out.println(
        Ansi.colorize(String.format("end: %s", range.getEndString()),
            Attribute.BRIGHT_YELLOW_TEXT()));
    System.out.println(
        Ansi.colorize(String.format("region: %s", region), Attribute.BRIGHT_YELLOW_TEXT()));
    System.out.println(
        Ansi.colorize(String.format("key: %s", key), Attribute.BRIGHT_YELLOW_TEXT()));

    System.out.println(
        StringUtils.repeat("=", resolution) + "  " + "total" + " " + range.getDuration().toMillis()
            + "ms");

    for (Span span : spans) {
      long leadingDuration =
          (span.getRange().getStart().getTime() - range.getStart().getTime()) * 1_000_000;
      int leadingSpace = (int) (leadingDuration * factor);
      System.out.print(StringUtils.repeat(" ", leadingSpace));

      long executingDuration = span.getRange().getDuration().getNano();
      int executingBar = (int) (executingDuration * factor);
      String bars = StringUtils.repeat("=", executingBar);
      if (executingDuration >= warningThreshold.toNanos()) {
        bars = Ansi.colorize(bars, Attribute.RED_TEXT());
      }
      System.out.print(bars);

      int tailingSpace = resolution - leadingSpace - executingBar + 1;
      System.out.print(StringUtils.repeat(" ", tailingSpace));

      System.out.println(
          " " + span.getRange().getDuration().toMillis() + "ms" + " " + span.getName());
    }
  }

  public SlowLog(
      String name,
      TimeRange timeRange,
      String region,
      String key,
      Span[] spans) {
    this.name = name;
    this.range = timeRange;
    this.region = region;
    this.key = key;
    this.spans = spans;
  }
}
