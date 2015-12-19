package backtype.storm.metric.api;

public class CountMetric implements IMetric {
  com.twitter.heron.api.metric.CountMetric delegate;

  public CountMetric() {
    delegate = new com.twitter.heron.api.metric.CountMetric();
  }
    
  public void incr() {
    delegate.incr();
  }

  public void incrBy(long incrementBy) {
    delegate.incrBy(incrementBy);
  }

  public Object getValueAndReset() {
    return delegate.getValueAndReset();
  }
}
