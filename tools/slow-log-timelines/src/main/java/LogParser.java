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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import slowlog.SlowLog;

public class LogParser {

  private static final String logPrefix = "SlowLog:";
  private static final Pattern logPattern = Pattern.compile(logPrefix + "\\{.+}");

  private final Scanner sc;

  public LogParser(String path) throws IOException {
    FileInputStream is = new FileInputStream(path);
    sc = new Scanner(is);
  }

  public List<SlowLog> parse() {
    List<SlowLog> logs = new ArrayList<>();
    while (sc.hasNextLine()) {
      String line = sc.nextLine();
      Matcher matcher = logPattern.matcher(line);
      if (matcher.find()) {
        String log = StringUtils.stripStart(matcher.group(), logPrefix);
        try {
          StringReader reader = new StringReader(log);
          SlowLog slowLog = SlowLog.fromJson(reader);
          logs.add(slowLog);
        } catch (Exception e) {
          System.out.println("Error parsing log: " + e.getMessage());
        }
      }
    }
    return logs;
  }
}
