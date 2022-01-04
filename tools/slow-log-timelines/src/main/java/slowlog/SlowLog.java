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

import com.diogonunes.jcolor.Ansi;
import com.diogonunes.jcolor.Attribute;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import java.io.Reader;
import java.time.Duration;
import org.apache.commons.lang3.StringUtils;

public class SlowLog {

  private final String name;
  private final TimeRange range;
  private final String region;
  private final String key;
  private final Span[] spans;

  public static SlowLog fromJson(Reader reader) throws JsonParseException {
    Gson gson =
        new GsonBuilder().registerTypeAdapter(SlowLog.class, new SlowLogDeserializer()).create();
    return gson.fromJson(reader, SlowLog.class);
  }

  public String getTimelines(int resolution, Duration threhold) {
    StringBuilder sb = new StringBuilder();
    float factor = (float) resolution / (float) range.getDuration().toNanos();
    Attribute attrColor = Attribute.YELLOW_TEXT();

    sb.append(Ansi.colorize(String.format("function: %s\n", name), attrColor));

    sb.append(Ansi.colorize(String.format("start: %s\n", range.getStartString()), attrColor));
    sb.append(Ansi.colorize(String.format("end: %s\n", range.getEndString()), attrColor));
    sb.append(Ansi.colorize(String.format("region: %s\n", region), attrColor));
    sb.append(Ansi.colorize(String.format("key: %s\n", key), attrColor));
    sb.append(StringUtils.repeat("=", resolution))
        .append("  total ")
        .append(range.getDuration().toMillis())
        .append("ms\n");

    for (Span span : spans) {
      long leadingDuration =
          (span.getRange().getStart().getTime() - range.getStart().getTime()) * 1_000_000;
      int leadingSpace = (int) (leadingDuration * factor);

      sb.append(StringUtils.repeat(" ", leadingSpace));

      long executingDuration = span.getRange().getDuration().toNanos();
      int executingBar = (int) (executingDuration * factor);
      String bars = StringUtils.repeat("=", executingBar);
      if (executingDuration >= threhold.toNanos()) {
        bars = Ansi.colorize(bars, Attribute.RED_TEXT());
      }
      sb.append(bars);

      int tailingSpace = resolution - leadingSpace - executingBar + 1;
      sb.append(StringUtils.repeat(" ", tailingSpace));

      sb.append(" ")
          .append(span.getRange().getDuration().toMillis())
          .append("ms ")
          .append(span.getName())
          .append("\n");
    }

    return sb.toString();
  }

  public SlowLog(String name, TimeRange timeRange, String region, String key, Span[] spans) {
    this.name = name;
    this.range = timeRange;
    this.region = region;
    this.key = key;
    this.spans = spans;
  }
}
