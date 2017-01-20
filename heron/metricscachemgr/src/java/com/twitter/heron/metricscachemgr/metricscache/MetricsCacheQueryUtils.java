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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.metricscachemgr.metricscache.datapoint.ExceptionDatapoint;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;

/**
 * define the query request and response format
 */
final class MetricsCacheQueryUtils {
  // logger
  private static final Logger LOG = Logger.getLogger(MetricsCacheQueryUtils.class.getName());

  private MetricsCacheQueryUtils() {
  }

  /**
   * compatible with com.twitter.heron.proto.tmaster.TopologyMaster.MetricRequest
   */
  public static MetricRequest fromProtobuf(TopologyMaster.MetricRequest request) {
    String componentName = request.getComponentName();

    MetricRequest outRequest = new MetricRequest();
    outRequest.componentNameInstanceId = new HashMap<>();
    if (request.getInstanceIdCount() == 0) {
      // empty list means all instances
      // 'null' means all instances
      outRequest.componentNameInstanceId.put(componentName, null);
    } else {
      Set<String> instances = new HashSet<>();
      // only one component
      outRequest.componentNameInstanceId.put(componentName, instances);
      // if there are instances specified
      instances.addAll(request.getInstanceIdList());
    }

    Set<String> metrics = new HashSet<>();
    outRequest.metricNames = metrics;
    if (request.getMetricCount() > 0) {
      metrics.addAll(request.getMetricList());
    } // empty list means no metrics

    // default: the whole time horizon
    outRequest.startTime = 0;
    outRequest.endTime = Long.MAX_VALUE;
    if (request.hasInterval()) { // endTime = now
      outRequest.endTime = System.currentTimeMillis();

      long interval = request.getInterval(); // in seconds
      if (interval <= 0) { // means all
        outRequest.startTime = 0;
      } else { // means [-interval, now]
        outRequest.startTime = outRequest.endTime - interval * 1000;
      }
    } else {
      outRequest.startTime = request.getExplicitInterval().getStart() * 1000;
      outRequest.endTime = request.getExplicitInterval().getEnd() * 1000;
    }

    // default: aggregate all metrics
    outRequest.aggregationGranularity = 0;
    if (request.hasMinutely()) {
      outRequest.aggregationGranularity = request.getMinutely() ? 1 : 0;
    }

    return outRequest;
  }

  /**
   * compatible with com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse
   */
  public static TopologyMaster.MetricResponse toProtobuf(MetricResponse response,
                                                         MetricRequest request) {
    TopologyMaster.MetricResponse.Builder builder =
        TopologyMaster.MetricResponse.newBuilder();
    builder.setInterval((request.endTime - request.startTime) / 1000); // in seconds

    // default OK if we have response to build already
    builder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK));

    // instanceId -> [metricName -> metricValue]
    // componentName is ignored, since there is only one component in the query
    Map<String, Map<String, List<MetricTimeRangeValue>>> aggregation =
        new HashMap<>();
    for (MetricDatum datum : response.metricList) {
      String instanceId = datum.instanceId;
      String metricName = datum.metricName;
      List<MetricTimeRangeValue> metricValue = datum.metricValue;
      // prepare
      if (!aggregation.containsKey(instanceId)) {
        aggregation.put(instanceId, new HashMap<String, List<MetricTimeRangeValue>>());
      }
      if (!aggregation.get(instanceId).containsKey(metricName)) {
        aggregation.get(instanceId).put(metricName, new ArrayList<MetricTimeRangeValue>());
      }
      // aggregate
      aggregation.get(instanceId).get(metricName).addAll(metricValue);
    }

    // add TaskMetric
    for (String instanceId : aggregation.keySet()) {
      TopologyMaster.MetricResponse.TaskMetric.Builder taskMetricBuilder =
          TopologyMaster.MetricResponse.TaskMetric.newBuilder();

      taskMetricBuilder.setInstanceId(instanceId);
      // add IndividualMetric
      for (String metricName : aggregation.get(instanceId).keySet()) {
        TopologyMaster.MetricResponse.IndividualMetric.Builder individualMetricBuilder =
            TopologyMaster.MetricResponse.IndividualMetric.newBuilder();

        individualMetricBuilder.setName(metricName);
        // add value|IntervalValue
        List<MetricTimeRangeValue> list = aggregation.get(instanceId).get(metricName);
        if (list.size() == 1) {
          LOG.info("get0 " + list.get(0) + "; value " + list.get(0).value);
          individualMetricBuilder.setValue(list.get(0).value);
        } else {
          for (MetricTimeRangeValue v : list) {
            TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.Builder
                intervalValueBuilder =
                TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.newBuilder();

            intervalValueBuilder.setValue(v.value);
            intervalValueBuilder.setInterval(TopologyMaster.MetricInterval.newBuilder()
                .setStart(v.startTime).setEnd(v.endTime));

            individualMetricBuilder.addIntervalValues(intervalValueBuilder);
          }// end IntervalValue
        }

        taskMetricBuilder.addMetric(individualMetricBuilder);
      }// end IndividualMetric

      builder.addMetric(taskMetricBuilder);
    }// end TaskMetric

    return builder.build();
  }

  // compatible with com.twitter.heron.proto.tmaster.TopologyMaster.ExceptionLogRequest
  public static ExceptionRequest fromProtobuf(TopologyMaster.ExceptionLogRequest request) {
    String componentName = request.getComponentName();

    ExceptionRequest outRequest = new ExceptionRequest();
    outRequest.componentNameInstanceId = new HashMap<>();
    Set<String> instances = new HashSet<>();
    // only one component
    outRequest.componentNameInstanceId.put(componentName, instances);

    if (request.getInstancesCount() > 0) {
      instances.addAll(request.getInstancesList());
    } // empty list means all instances

    return outRequest;
  }

  // compatible with com.twitter.heron.proto.tmaster.TopologyMaster.ExceptionLogResponse
  public static TopologyMaster.ExceptionLogResponse toProtobuf(ExceptionResponse response) {
    TopologyMaster.ExceptionLogResponse.Builder builder =
        TopologyMaster.ExceptionLogResponse.newBuilder();
    // default OK if we have response to build already
    builder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK));

    for (ExceptionDatapoint e : response.exceptionDatapointList) {
      TopologyMaster.TmasterExceptionLog.Builder exceptionBuilder =
          TopologyMaster.TmasterExceptionLog.newBuilder();
      // ExceptionDatapoint
      exceptionBuilder.setComponentName(e.componentName);
      exceptionBuilder.setHostname(e.hostname);
      exceptionBuilder.setInstanceId(e.instanceId);
      // ExceptionData
      exceptionBuilder.setStacktrace(e.stackTrace);
      exceptionBuilder.setLasttime(e.lastTime);
      exceptionBuilder.setFirsttime(e.firstTime);
      exceptionBuilder.setCount(e.count);
      exceptionBuilder.setLogging(e.logging);

      builder.addExceptions(exceptionBuilder);
    }

    return builder.build();
  }


  public static class ExceptionRequest {
    public Map<String, Set<String>> componentNameInstanceId;
  }

  public static class ExceptionResponse {
    public List<ExceptionDatapoint> exceptionDatapointList;
  }


  public static class MetricTimeRangeValue {
    public long startTime;
    public long endTime;
    public String value;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[")
          .append(startTime).append("-").append(endTime)
          .append(":")
          .append(value)
          .append("]");
      return sb.toString();
    }
  }

  public static class MetricDatum {
    public String componentName;
    public String instanceId;
    public String metricName;
    public List<MetricTimeRangeValue> metricValue;
  }

  public static class MetricRequest {
    // The instance ids to get the stats from
    // If nothing is specified, we will get from
    // all the instances of the component name
    public Map<String, Set<String>> componentNameInstanceId;
    // What set of metrics you are interested in
    // Example is __emit-count/default
    public Set<String> metricNames;
    // what timeframe data in milliseconds
    public long startTime;
    public long endTime;
    // aggregation granularity
    // 0: default, aggregate all metrics
    // 1: aggregate metrics by bucket
    // 2: no aggregation; return raw metrics
    public int aggregationGranularity;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{")
          .append("[").append(startTime).append("-").append(endTime)
          .append(":").append(aggregationGranularity).append("]")
          .append("[");
      for (String c : componentNameInstanceId.keySet()) {
        sb.append(c).append("->(");
        if (componentNameInstanceId.get(c) == null) {
          sb.append("null");
        } else {
          for (String i : componentNameInstanceId.get(c)) {
            sb.append(i).append(",");
          }
        }
        sb.append("),");
      }
      sb.append("]")
          .append("[");
      for (String name : metricNames) {
        sb.append(name).append(",");
      }
      sb.append("]")
          .append("}");
      return sb.toString();
    }
  }

  public static class MetricResponse {
    public List<MetricDatum> metricList;
  }
}
