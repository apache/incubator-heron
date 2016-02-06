package com.twitter.heron.spi.metricsmgr.sink;

/**
 * Context needed for an IMetricsSink to init.
 * <p/>
 * We distinguish config and context carefully:
 * Config is populated from yaml file and would not be changed anymore,
 * while context is populated in run-time.
 */

public interface SinkContext {
  String getTopologyName();

  String getMetricsMgrId();

  String getSinkId();

  void exportCountMetric(String metricName, long delta);
}
