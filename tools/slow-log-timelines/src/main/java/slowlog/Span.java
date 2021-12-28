package slowlog;

public class Span {


  private final String name;
  private final TimeRange range;

  public String getName() {
    return name;
  }

  public TimeRange getRange() {
    return range;
  }


  public Span(String name, TimeRange range) {
    this.name = name;
    this.range = range;
  }
}
