package org.apache.heron.spouts.kafka;

import com.twitter.heron.api.metric.IMetric;
import org.apache.kafka.common.Metric;

public class KafkaMetricDecorator<M extends Metric> implements IMetric<Object> {
    private M metric;

    KafkaMetricDecorator(M metric) {
        this.metric = metric;
    }

    @Override
    public Object getValueAndReset() {
        return metric.metricValue();
    }
}
