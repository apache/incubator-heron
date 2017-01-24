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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.metricscachemgr.metricscache.datapoint.ExceptionDatapoint;
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

  private static final Logger LOG = Logger.getLogger(CacheCore.class.getName());

  private CacheCore cache = null;
  private MetricsFilter metricNameType = null;

  public MetricsCache(SystemConfig systemConfig, MetricsSinksConfig sinksConfig,
                      WakeableLooper looper) {
    // metadata
    metricNameType = new MetricsFilter();
    Map<String, Object> sinksTMaster = sinksConfig.getConfigForSink(METRICS_SINKS_TMASTER_SINK);
    @SuppressWarnings("unchecked")
    Map<String, String> metricsTypes =
        (Map<String, String>) sinksTMaster.get(METRICS_SINKS_TMASTER_METRICS);
    for (String metricName : metricsTypes.keySet()) {
      metricNameType.setMetricToType(metricName, translateFromString(metricsTypes.get(metricName)));
    }
    //
    long maxIntervalSec = systemConfig.getTmasterMetricsCollectorMaximumIntervalMin() * 60;
    long purgeIntervalSec = systemConfig.getTmasterMetricsCollectorPurgeIntervalSec();
    long maxExceptions = systemConfig.getTmasterMetricsCollectorMaximumException();

    cache = new CacheCore(maxIntervalSec, purgeIntervalSec, maxExceptions);

    cache.startPurge(looper);
  }

  private static TopologyMaster.MetricResponse.Builder buildResponseNotOk(String message) {
    TopologyMaster.MetricResponse.Builder builder =
        TopologyMaster.MetricResponse.newBuilder();
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
  public void addMetrics(TopologyMaster.PublishMetrics metrics) {
    cache.addMetricException(metrics);
  }

  /**
   * for inside SLA process component query
   *
   * @param request query statement
   * @return metric list
   */
  public MetricsCacheQueryUtils.MetricResponse getMetrics(
      MetricsCacheQueryUtils.MetricRequest request) {
    return cache.getMetrics(request, metricNameType);
  }

  /**
   * for inside SLA process component query
   *
   * @param request query statement
   * @return exception list
   */
  public MetricsCacheQueryUtils.ExceptionResponse getExceptions(
      MetricsCacheQueryUtils.ExceptionRequest request) {
    return cache.getExceptions(request);
  }

  /**
   * compatible with tmaster interface
   *
   * @param request query request defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyMaster.ExceptionLogResponse getExceptions(
      TopologyMaster.ExceptionLogRequest request) {
    MetricsCacheQueryUtils.ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    MetricsCacheQueryUtils.ExceptionResponse response1 = cache.getExceptions(request1);
    TopologyMaster.ExceptionLogResponse response = MetricsCacheQueryUtils.toProtobuf(response1);
    return response;
  }

  private MetricsCacheQueryUtils.ExceptionResponse summarizeException(
      MetricsCacheQueryUtils.ExceptionResponse response1) {
    Map<String, ExceptionDatapoint> exceptionSummary = new HashMap<>();
    for (ExceptionDatapoint edp : response1.exceptionDatapointList) {
      // Get classname by splitting on first colon
      int pos = edp.stackTrace.indexOf(':');
      if (pos >= 0) {
        String className = edp.stackTrace.substring(0, pos);
        if (!exceptionSummary.containsKey(className)) {
          ExceptionDatapoint edp2 = new ExceptionDatapoint();
          edp2.componentName = edp.componentName;
          edp2.hostname = edp.hostname;
          edp2.instanceId = edp.instanceId;
          edp2.firstTime = edp.firstTime;
          edp2.lastTime = edp.lastTime;
          edp2.count = edp.count;
          edp2.logging = edp.logging;
          edp2.stackTrace = className;
          exceptionSummary.put(className, edp2);
        } else {
          ExceptionDatapoint edp3 = exceptionSummary.get(className);
          edp3.count += edp.count;
          edp3.firstTime =
              edp3.firstTime.compareTo(edp.firstTime) < 0 ? edp3.firstTime : edp.firstTime;
          edp3.lastTime = edp3.lastTime.compareTo(edp.lastTime) > 0 ? edp3.lastTime : edp.lastTime;
        }
      }
    }
    MetricsCacheQueryUtils.ExceptionResponse ret = new MetricsCacheQueryUtils.ExceptionResponse();
    ret.exceptionDatapointList.addAll(exceptionSummary.values());
    return ret;
  }

  /**
   * compatible with tmaster interface
   *
   * @param request query statement defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyMaster.ExceptionLogResponse getExceptionsSummary(
      TopologyMaster.ExceptionLogRequest request) {
    MetricsCacheQueryUtils.ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    MetricsCacheQueryUtils.ExceptionResponse response1 = cache.getExceptions(request1);
    MetricsCacheQueryUtils.ExceptionResponse response2 = summarizeException(response1);
    TopologyMaster.ExceptionLogResponse response = MetricsCacheQueryUtils.toProtobuf(response2);
    return response;
  }

  /**
   * compatible with tmaster interface
   *
   * @param request query statement defined in protobuf
   * @return query result defined in protobuf
   */
  public TopologyMaster.MetricResponse getMetrics(TopologyMaster.MetricRequest request) {
    String componentName = request.getComponentName();
    if (!cache.existComponentInstance(componentName, null)) {
      return buildResponseNotOk("Unknown component: " + componentName).build();
    }
    if (request.getInstanceIdCount() > 0) {
      for (String instanceId : request.getInstanceIdList()) {
        if (!cache.existComponentInstance(componentName, instanceId)) {
          return buildResponseNotOk("Unknown instance: " + instanceId).build();
        }
      }
    }
    if (!request.hasInterval() && !request.hasExplicitInterval()) {
      return buildResponseNotOk("No purgeIntervalSec or explicit purgeIntervalSec set").build();
    }
    // query
    MetricsCacheQueryUtils.MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    MetricsCacheQueryUtils.MetricResponse response1 = cache.getMetrics(request1, metricNameType);
    TopologyMaster.MetricResponse response = MetricsCacheQueryUtils.toProtobuf(response1, request1);
    return response;
  }
}
