package org.apache.storm.metric.api;

public interface IMetric {
    public Object getValueAndReset();
}
