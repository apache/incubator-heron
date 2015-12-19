package com.twitter.heron.api.metric;

public interface IMetric {
    public Object getValueAndReset();
}
