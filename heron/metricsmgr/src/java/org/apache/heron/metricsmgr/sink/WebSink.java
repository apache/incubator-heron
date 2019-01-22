/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.metricsmgr.sink;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.metricsmgr.MetricsUtil;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;


/**
 * A metrics sink that publishes metrics on a http endpoint
 */
public class WebSink extends AbstractWebSink  {
  private static final Logger LOG = Logger.getLogger(WebSink.class.getName());

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // If flat-metrics is false, the metrics will be grouped by the metrics
  // source source,
  // if flat-metrics is true, the metric names will be prefixed by the source
  private static final String KEY_FLAT_METRICS = "flat-metrics";
  private static final boolean DEFAULT_FLATTEN_METRICS = true;

  // Include the topology name in the metric name
  private static final String KEY_INCLUDE_TOPOLOGY_NAME = "include-topology-name";
  private static final boolean DEFAULT_INCLUDE_TOPOLOGY_NAME = false;

  // This is the cache that is used to serve the metrics
  private Cache<String, Object> metricsCache;

  private boolean flattenMetrics;
  private boolean shouldIncludeTopologyName;

  public WebSink() {
    super();
  }

  @VisibleForTesting
  WebSink(Ticker cacheTicker) {
    super(cacheTicker);
  }

  Cache<String, Object> getMetricsCache() {
    return metricsCache;
  }

  @Override
  void initialize(Map<String, Object> configuration, SinkContext context) {

    flattenMetrics = TypeUtils.getBoolean(
        configuration.getOrDefault(KEY_FLAT_METRICS, DEFAULT_FLATTEN_METRICS));

    shouldIncludeTopologyName = TypeUtils.getBoolean(
        configuration.getOrDefault(KEY_INCLUDE_TOPOLOGY_NAME, DEFAULT_INCLUDE_TOPOLOGY_NAME));

    metricsCache = createCache();
  }

  @Override
  byte[] generateResponse() throws IOException {
    metricsCache.cleanUp();
    return MAPPER.writeValueAsString(metricsCache.asMap()).getBytes();
  }

  @Override
  public void processRecord(MetricsRecord record) {
    final String[] sources = MetricsUtil.splitRecordSource(record);
    final String source;

    if (sources.length > 2) {
      source = shouldIncludeTopologyName
          ? String.format("%s/%s/%s", getTopologyName(), sources[1], sources[2])
          : String.format("/%s/%s", sources[1], sources[2]);
    } else {
      source = shouldIncludeTopologyName
          ? String.format("%s/%s", getTopologyName(), record.getSource())
          : String.format("/%s", record.getSource());
    }

    if (flattenMetrics) {
      metricsCache.putAll(processMetrics(source + "/", record.getMetrics()));
    } else {
      Map<String, Double> sourceCache;
      Object sourceObj = metricsCache.getIfPresent(source);
      if (sourceObj instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Double> castObj = (Map<String, Double>) sourceObj;
        sourceCache = castObj;
      } else {
        final Cache<String, Double> newSourceCache = createCache();
        sourceCache = newSourceCache.asMap();
      }
      sourceCache.putAll(processMetrics("", record.getMetrics()));
      metricsCache.put(source, sourceCache);
    }
  }

  /**
   * Helper to prefix metric names, convert metric value to double and return as map
   *
   * @param prefix
   * @param metrics
   * @return Map of metric name to metric value
   */
  static Map<String, Double> processMetrics(String prefix, Iterable<MetricsInfo> metrics) {
    Map<String, Double> map = new HashMap<>();
    for (MetricsInfo r : metrics) {
      try {
        map.put(prefix + r.getName(), Double.valueOf(r.getValue()));
      } catch (NumberFormatException ne) {
        LOG.log(Level.SEVERE, "Could not parse metric, Name: "
            + r.getName() + " Value: " + r.getValue(), ne);
      }
    }
    return map;
  }
}
