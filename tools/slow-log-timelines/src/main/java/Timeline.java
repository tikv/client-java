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

import java.time.Duration;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import slowlog.SlowLog;

public class Timeline implements Callable<Integer> {

  @Option(names = {"--resolution"}, defaultValue = "70", description = "resolution of timelines")
  private int resolution;

  @Option(names = {
      "--threshold"}, defaultValue = "10", description = "threshold of warning time, in ms")
  int threshold;

  @Parameters(index = "0", description = "The log file to parse")
  private String path;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Timeline()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    for (SlowLog log : new LogParser(path).parse()) {
      String timelines = log.getTimelines(resolution, Duration.ofMillis(threshold));
      System.out.print(timelines);
    }
    return 0;
  }
}
