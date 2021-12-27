import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import slowlog.SlowLog;

public class Main {

  public static void main(String[] args) throws Exception {
    for (SlowLog log : new LogParser("./example.log").parse()) {
      log.dump();
    }
  }
}
