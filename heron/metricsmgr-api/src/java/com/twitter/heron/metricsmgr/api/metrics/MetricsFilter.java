package com.twitter.heron.metricsmgr.api.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A MetricsFilter which could be used to:
 * 1. Specify the metric-prefix or metric-name we need to keep
 * 2. Query whether a metric is needed by contains method
 * 3. Get the MetricAggregationType by getAggregationType method
 * 4. Filter the needed MetricsInfo
 */
public class MetricsFilter {
  public enum MetricAggregationType {
    UNKNOWN, SUM, AVG, LAST;
  }

  private final Map<String, MetricAggregationType> prefixToType =
      new HashMap<String, MetricAggregationType>();

  public void setPrefixToType(String prefix, MetricAggregationType type) {
    prefixToType.put(prefix, type);
  }

  public void setMetricToType(String metricName, MetricAggregationType type) {
    setPrefixToType(metricName, type);
  }

  public boolean contains(String metricName) {
    for (String prefix : prefixToType.keySet()) {
      if (metricName.contains(prefix)) {
        return true;
      }
    }
    return false;
  }

  // Return an immutable view of filtered metrics
  public Iterable<MetricsInfo> filter(Iterable<MetricsInfo> metricsInfos) {
    List<MetricsInfo> metricsFiltered = new ArrayList<MetricsInfo>();
    for (MetricsInfo metricsInfo : metricsInfos) {
      if (contains(metricsInfo.getName())) {
        metricsFiltered.add(metricsInfo);
      }
    }

    return metricsFiltered;
  }

  public MetricAggregationType getAggregationType(String metricName) {
    for (String prefix : prefixToType.keySet()) {
      if (metricName.contains(prefix)) {
        return prefixToType.get(prefix);
      }
    }
    return MetricAggregationType.UNKNOWN;
  }
}
