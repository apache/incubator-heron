package com.twitter.heron.metricsmgr.api.metrics;

/**
 * An immutable class providing a view of MetricsInfo
 * The value is in type String, and IMetricsSink would determine how to parse it.
 */
public class MetricsInfo {
  private final String name;
  private final String value;

  public MetricsInfo(String name, String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Get the name of the metric
   *
   * @return the name of the metric
   */
  public String getName() {
    return name;
  }

  /**
   * Get the value of the metric
   *
   * @return the value of the metric
   */
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%s = %s", getName(), getValue());
  }
}
