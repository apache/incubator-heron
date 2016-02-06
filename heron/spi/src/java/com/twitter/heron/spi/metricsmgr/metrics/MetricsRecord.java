package com.twitter.heron.spi.metricsmgr.metrics;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * An immutable snapshot of metrics and exception log with a timestamp and other meta data.
 */
public class MetricsRecord {
  private static final String DEFAULT_CONTEXT = "default";
  private final long timestamp;

  private final String source;

  private final Iterable<MetricsInfo> metrics;

  private final Iterable<ExceptionInfo> exceptions;

  private final String context;

  public MetricsRecord(String source,
                       Iterable<MetricsInfo> metrics,
                       Iterable<ExceptionInfo> exceptions) {
    this(source, metrics, exceptions, DEFAULT_CONTEXT);
  }

  public MetricsRecord(String source,
                       Iterable<MetricsInfo> metrics,
                       Iterable<ExceptionInfo> exceptions,
                       String context) {
    this(System.currentTimeMillis(), source, metrics, exceptions, context);
  }

  public MetricsRecord(long timestamp, String source,
                       Iterable<MetricsInfo> metrics,
                       Iterable<ExceptionInfo> exceptions,
                       String context) {
    this.source = source;
    this.timestamp = timestamp;
    this.context = context;
    this.metrics = metrics;
    this.exceptions = exceptions;
  }

  /**
   * Get the timestamp of the metrics
   *
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Get the name of source generating metrics
   *
   * @return the name of source
   */
  public String getSource() {
    return source;
  }

  /**
   * Get the metrics of the record
   *
   * @return an immutable iterable interface for MetricInfo
   */
  public Iterable<MetricsInfo> getMetrics() {
    return metrics;
  }

  /**
   * Get the exception logs of the record
   *
   * @return an immutable iterable interface for ExceptionInfo
   */
  public Iterable<ExceptionInfo> getExceptions() {
    return exceptions;
  }

  /**
   * @return the context name of the metrics record
   */
  public String getContext() {
    return context;
  }

  @Override
  public String toString() {
    // Pack metrics as a map
    Map<String, String> metrics = new HashMap<String, String>();
    for (MetricsInfo metricsInfo : getMetrics()) {
      metrics.put(metricsInfo.getName(), metricsInfo.getValue());
    }

    // Pack exceptions as a list of map
    LinkedList<Object> exceptions = new LinkedList<Object>();
    for (ExceptionInfo exceptionInfo : getExceptions()) {
      Map<String, Object> exception = new HashMap<String, Object>();
      exception.put("firstTime", exceptionInfo.getFirstTime());
      exception.put("lastTime", exceptionInfo.getLastTime());
      exception.put("logging", exceptionInfo.getLogging());
      exception.put("stackTrace", exceptionInfo.getStackTrace());
      exception.put("count", exceptionInfo.getCount());
      exceptions.add(exception);
    }

    // Pack the whole MetricsRecord as a map
    Map<String, Object> result = new HashMap<String, Object>();
    result.put("timestamp", getTimestamp());
    result.put("source", getSource());
    result.put("context", getContext());
    result.put("metrics", metrics);
    result.put("exceptions", exceptions);

    return result.toString();
  }
}
