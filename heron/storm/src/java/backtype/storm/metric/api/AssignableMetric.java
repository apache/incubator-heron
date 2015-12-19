package backtype.storm.metric.api;

public class AssignableMetric implements IMetric {
  private com.twitter.heron.api.metric.AssignableMetric delegate;

  public AssignableMetric(Object value) {
    delegate = new com.twitter.heron.api.metric.AssignableMetric(value);
  }

  public void setValue(Object value) {
    delegate.setValue(value);
  }

  public Object getValueAndReset() {
    return delegate.getValueAndReset();
  }
}
