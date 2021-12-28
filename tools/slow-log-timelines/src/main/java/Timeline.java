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
