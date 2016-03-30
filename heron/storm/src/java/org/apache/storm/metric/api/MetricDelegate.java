package org.apache.storm.metric.api;

public class MetricDelegate implements com.twitter.heron.api.metric.IMetric {
  private IMetric delegate;

  public MetricDelegate(IMetric delegate) {
    this.delegate = delegate;
  }
  
  @Override
  public Object getValueAndReset() {
    return delegate.getValueAndReset();
  }
}
