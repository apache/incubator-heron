// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.metricscachemgr.metricscache;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.metricsmgr.MetricsSinksConfig;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

/**
 * compatible with tmaster interface
 * see heron/tmaster/src/cpp/manager/tmetrics-collector.h
 */
public class MetricsCache {
  public static final String METRICS_SINKS_TMASTER_SINK = "tmaster-sink";
  public static final String METRICS_SINKS_TMASTER_METRICS = "tmaster-metrics-type";

  // logger
  private static final Logger LOG = Logger.getLogger(CacheCore.class.getName());

  private CacheCore cache = null;
  private MetricsFilter metricNameType = null;

  /**
   * constructor
   *
   * @param systemConfig heron config
   * @param sinksConfig sink config
   */
  public MetricsCache(SystemConfig systemConfig, MetricsSinksConfig sinksConfig) {
    // metadata
    metricNameType = new MetricsFilter();
    Map<String, Object> sinksTmaster = sinksConfig.getConfigForSink(METRICS_SINKS_TMASTER_SINK);
    @SuppressWarnings("unchecked")
    Map<String, String> metricsType =
        (Map<String, String>) sinksTmaster.get(METRICS_SINKS_TMASTER_METRICS);
    for (Map.Entry<String, String> e : metricsType.entrySet()) {
      metricNameType.setMetricToType(e.getKey(), TranslateFromString(e.getValue()));
    }
    //
    long maxInterval = systemConfig.getTmasterMetricsCollectorMaximumIntervalMin() * 60;
    long interval = systemConfig.getTmasterMetricsCollectorPurgeIntervalSec();
    long maxException = systemConfig.getTmasterMetricsCollectorMaximumException();

    cache = new CacheCore(maxInterval, interval, maxException);

    cache.startPurge();
  }

  private MetricsFilter.MetricAggregationType TranslateFromString(String type) {
    if ("SUM".equals(type)) {
      return MetricsFilter.MetricAggregationType.SUM;
    } else if ("AVG".equals(type)) {
      return MetricsFilter.MetricAggregationType.AVG;
    } else if ("LAST".equals(type)) {
      return MetricsFilter.MetricAggregationType.LAST;
    } else {
      LOG.log(Level.SEVERE, "Unknown metrics type in metrics sinks " + type);
      return MetricsFilter.MetricAggregationType.UNKNOWN;
    }
  }

  /**
   * sink publishes metrics and exceptions to this interface
   *
   * @param metrics message from sinks
   */
  public void AddMetric(TopologyMaster.PublishMetrics metrics) {
    cache.AddMetricException(metrics);
  }

  /**
   * for inside SLA process component query
   *
   * @param request query statement
   * @return metric list
   */
  public MetricsCacheQueryUtils.MetricResponse GetMetrics(
      MetricsCacheQueryUtils.MetricRequest request) {
    return cache.GetMetrics(request, metricNameType);
  }

  /**
   * for inside SLA process component query
   *
   * @param request query statement
   * @return exception list
   */
  public MetricsCacheQueryUtils.ExceptionResponse GetExceptions(
      MetricsCacheQueryUtils.ExceptionRequest request) {
    return cache.GetExceptions(request);
  }

  /**
   * compatible with tmaster interface
   *
   * @param request query request defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyMaster.ExceptionLogResponse GetExceptions(
      TopologyMaster.ExceptionLogRequest request) {
    MetricsCacheQueryUtils.ExceptionRequest request1 = MetricsCacheQueryUtils.Convert(request);
    MetricsCacheQueryUtils.ExceptionResponse response1 = cache.GetExceptions(request1);
    TopologyMaster.ExceptionLogResponse response = MetricsCacheQueryUtils.Convert(response1);
    return response;
  }

  private MetricsCacheQueryUtils.ExceptionResponse SummarizeException(
      MetricsCacheQueryUtils.ExceptionResponse response1) {
    return null;
  }

  /**
   * compatible with tmaster interface
   *
   * @param request query statement defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyMaster.ExceptionLogResponse GetExceptionsSummary(
      TopologyMaster.ExceptionLogRequest request) {
    MetricsCacheQueryUtils.ExceptionRequest request1 = MetricsCacheQueryUtils.Convert(request);
    MetricsCacheQueryUtils.ExceptionResponse response1 = cache.GetExceptions(request1);
    MetricsCacheQueryUtils.ExceptionResponse response2 = SummarizeException(response1);
    TopologyMaster.ExceptionLogResponse response = MetricsCacheQueryUtils.Convert(response2);
    return response;
  }

  /**
   * compatible with tmaster interface
   *
   * @param request query statement defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyMaster.MetricResponse GetMetrics(TopologyMaster.MetricRequest request) {
    String componentName = request.getComponentName();
    if (!cache.existComponentInstance(componentName, null)) {
      TopologyMaster.MetricResponse.Builder builder =
          TopologyMaster.MetricResponse.newBuilder();
      builder.setStatus(Common.Status.newBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("Unknown component: " + componentName));
      return builder.build();
    }
    if (request.getInstanceIdCount() > 0) {
      for (String instanceId : request.getInstanceIdList()) {
        if (!cache.existComponentInstance(componentName, instanceId)) {
          TopologyMaster.MetricResponse.Builder builder =
              TopologyMaster.MetricResponse.newBuilder();
          builder.setStatus(Common.Status.newBuilder()
              .setStatus(Common.StatusCode.NOTOK)
              .setMessage("Unknown instance: " + instanceId));
          return builder.build();
        }
      }
    }
    if (!request.hasInterval() && !request.hasExplicitInterval()) {
      TopologyMaster.MetricResponse.Builder builder =
          TopologyMaster.MetricResponse.newBuilder();
      builder.setStatus(Common.Status.newBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("No interval or explicit interval set"));
      return builder.build();
    }
    // query
    MetricsCacheQueryUtils.MetricRequest request1 = MetricsCacheQueryUtils.Convert(request);
    MetricsCacheQueryUtils.MetricResponse response1 = cache.GetMetrics(request1, metricNameType);
    TopologyMaster.MetricResponse response = MetricsCacheQueryUtils.Convert(response1, request1);
    return response;
  }
}
