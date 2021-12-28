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
