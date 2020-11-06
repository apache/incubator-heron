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

package org.apache.heron.metricscachemgr.metricscache;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.common.basics.WakeableLooper;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionDatum;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionRequest;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionResponse;
import org.apache.heron.metricscachemgr.metricscache.query.MetricRequest;
import org.apache.heron.metricscachemgr.metricscache.query.MetricResponse;
import org.apache.heron.metricsmgr.MetricsSinksConfig;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.metricsmgr.metrics.MetricsFilter;

/**
 * Interface for the cache core
 * providing compatible interface with tmanager
 * see heron/tmanager/src/cpp/manager/tmetrics-collector.h
 */
public class MetricsCache {
  public static final String METRICS_SINKS_METRICSCACHE_SINK = "metricscache-sink";
  public static final String METRICS_SINKS_METRICSCACHE_METRICS = "metricscache-metrics-type";

  private static final Logger LOG = Logger.getLogger(CacheCore.class.getName());

  private final CacheCore cache;
  private final MetricsFilter metricNameType;

  public MetricsCache(SystemConfig systemConfig, MetricsSinksConfig sinksConfig,
                      WakeableLooper looper) {
    // metadata
    metricNameType = new MetricsFilter();
    Map<String, Object> sinksTManager =
        sinksConfig.getConfigForSink(METRICS_SINKS_METRICSCACHE_SINK);
    @SuppressWarnings("unchecked")
    Map<String, String> metricsTypes =
        (Map<String, String>) sinksTManager.get(METRICS_SINKS_METRICSCACHE_METRICS);
    for (String metricName : metricsTypes.keySet()) {
      metricNameType.setMetricToType(metricName, translateFromString(metricsTypes.get(metricName)));
    }

    Duration maxInterval = systemConfig.getTmanagerMetricsCollectorMaximumInterval();
    Duration purgeInterval = systemConfig.getTmanagerMetricsCollectorPurgeInterval();
    long maxExceptions = systemConfig.getTmanagerMetricsCollectorMaximumException();

    cache = new CacheCore(maxInterval, purgeInterval, maxExceptions);

    cache.startPurge(looper);
  }

  private static TopologyManager.MetricResponse.Builder buildResponseNotOk(String message) {
    TopologyManager.MetricResponse.Builder builder =
        TopologyManager.MetricResponse.newBuilder();
    builder.setStatus(Common.Status.newBuilder()
        .setStatus(Common.StatusCode.NOTOK)
        .setMessage(message));
    return builder;
  }

  private MetricsFilter.MetricAggregationType translateFromString(String type) {
    try {
      return MetricsFilter.MetricAggregationType.valueOf(type);
    } catch (IllegalArgumentException e) {
      LOG.log(Level.SEVERE, "Unknown metrics type in metrics sinks " + type + "; " + e);
      return MetricsFilter.MetricAggregationType.UNKNOWN;
    }
  }

  /**
   * sink publishes metrics and exceptions to this interface
   *
   * @param metrics message from sinks
   */
  public void addMetrics(TopologyManager.PublishMetrics metrics) {
    cache.addMetricException(metrics);
  }

  /**
   * for inside SLA process component query
   *
   * @param request query statement
   * @return metric list
   */
  public MetricResponse getMetrics(MetricRequest request) {
    return cache.getMetrics(request, metricNameType);
  }

  /**
   * for inside SLA process component query
   *
   * @param request query statement
   * @return exception list
   */
  public ExceptionResponse getExceptions(ExceptionRequest request) {
    return cache.getExceptions(request);
  }

  /**
   * compatible with tmanager interface
   *
   * @param request query request defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyManager.ExceptionLogResponse getExceptions(
      TopologyManager.ExceptionLogRequest request) {
    ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    ExceptionResponse response1 = cache.getExceptions(request1);
    TopologyManager.ExceptionLogResponse response = MetricsCacheQueryUtils.toProtobuf(response1);
    return response;
  }

  private ExceptionResponse summarizeException(ExceptionResponse response1) {
    Map<String, ExceptionDatum> exceptionSummary = new HashMap<>();
    for (ExceptionDatum edp : response1.getExceptionDatapointList()) {
      // Get classname by splitting on first colon
      int pos = edp.getStackTrace().indexOf(':');
      if (pos >= 0) {
        String className = edp.getStackTrace().substring(0, pos);
        if (!exceptionSummary.containsKey(className)) {
          exceptionSummary.put(className,
              new ExceptionDatum(edp.getComponentName(), edp.getInstanceId(), edp.getHostname(),
                  className, edp.getLastTime(), edp.getFirstTime(),
                  edp.getCount(), edp.getLogging()));
        } else {
          ExceptionDatum edp3 = exceptionSummary.get(className);
          // update count and time
          int count = edp3.getCount() + edp.getCount();
          String firstTime = edp3.getFirstTime();
          String lastTime = edp.getLastTime(); // should assure the time ?
          // put it back in summary
          exceptionSummary.put(className,
              new ExceptionDatum(edp3.getComponentName(), edp3.getInstanceId(), edp3.getHostname(),
                  edp3.getStackTrace(), lastTime, firstTime, count, edp3.getLogging()));
        }
      }
    }
    ExceptionResponse ret = new ExceptionResponse(exceptionSummary.values());
    return ret;
  }

  /**
   * compatible with tmanager interface
   *
   * @param request query statement defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyManager.ExceptionLogResponse getExceptionsSummary(
      TopologyManager.ExceptionLogRequest request) {
    ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    ExceptionResponse response1 = cache.getExceptions(request1);
    ExceptionResponse response2 = summarizeException(response1);
    TopologyManager.ExceptionLogResponse response = MetricsCacheQueryUtils.toProtobuf(response2);
    return response;
  }

  /**
   * compatible with tmanager interface
   *
   * @param request query statement defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyManager.MetricResponse getMetrics(TopologyManager.MetricRequest request) {
    String componentName = request.getComponentName();
    if (!cache.componentInstanceExists(componentName, null)) {
      return buildResponseNotOk(
          String.format("Unknown component %s found in MetricRequest %s", componentName, request)
      ).build();
    }
    if (request.getInstanceIdCount() > 0) {
      for (String instanceId : request.getInstanceIdList()) {
        if (!cache.componentInstanceExists(componentName, instanceId)) {
          return buildResponseNotOk(
              String.format("Unknown instance %s found in MetricRequest %s", instanceId, request)
          ).build();
        }
      }
    }
    if (!request.hasInterval() && !request.hasExplicitInterval()) {
      return buildResponseNotOk("No purgeIntervalSec or explicit purgeIntervalSec set").build();
    }

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    MetricResponse response1 = cache.getMetrics(request1, metricNameType);
    TopologyManager.MetricResponse response =
        MetricsCacheQueryUtils.toProtobuf(response1, request1);
    return response;
  }
}
