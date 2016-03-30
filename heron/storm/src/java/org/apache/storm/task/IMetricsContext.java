package org.apache.storm.task;

import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.ReducedMetric;


public interface IMetricsContext {
    <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs);
    ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs);
    CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs);  
}
