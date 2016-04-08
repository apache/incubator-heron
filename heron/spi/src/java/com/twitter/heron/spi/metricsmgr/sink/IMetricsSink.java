package com.twitter.heron.spi.metricsmgr.sink;

import java.util.Map;

import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;

/**
 * The metrics sink interface. <p>
 * Implementations of this interface consume the {@link MetricsRecord} gathered
 * by Metrics Manager. The Metrics Manager pushes the {@link MetricsRecord} to the sink using
 * {@link #processRecord(MetricsRecord)} method.
 * And {@link #flush()} is called at an interval according to the configuration
 */
public interface IMetricsSink extends AutoCloseable {
  /**
   * Initialize the MetricsSink
   *
   * @param conf An unmodifiableMap containing basic configuration
   * @param context context objects for Sink to init
   * Attempts to modify the returned map,
   * whether direct or via its collection views, result in an UnsupportedOperationException.
   */
  void init(Map<String, Object> conf, SinkContext context);

  /**
   * Process a metrics record in the sink
   *
   * @param record the record to put
   */
  void processRecord(MetricsRecord record);

  /**
   * Flush any buffered metrics
   * It would be called at an interval according to the configuration
   */
  void flush();

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();
}
