package com.twitter.heron.metricsmgr.sink;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

/**
 * Context needed for an IMetricsSink to init.
 * <p/>
 * We distinguish config and context carefully:
 * Config is populated from yaml file and would not be changed anymore,
 * while context is populated in run-time.
 */

public class SinkContextImpl implements SinkContext {

  private final MultiCountMetric internalMultiCountMetrics;

  private final String sinkId;

  private final String metricsmgrId;

  private final String topologyName;

  public SinkContextImpl(String topologyName,
                         String metricsmgrId,
                         String sinkId,
                         MultiCountMetric internalMultiCountMetrics) {
    this.topologyName = topologyName;
    this.metricsmgrId = metricsmgrId;
    this.sinkId = sinkId;
    this.internalMultiCountMetrics = internalMultiCountMetrics;
  }

  @Override
  public String getTopologyName() {
    return topologyName;
  }

  @Override
  public String getMetricsMgrId() {
    return metricsmgrId;
  }

  @Override
  public String getSinkId() {
    return sinkId;
  }

  @Override
  public void exportCountMetric(String metricName, long delta) {
    internalMultiCountMetrics.scope(metricName).incrBy(delta);
  }
}
