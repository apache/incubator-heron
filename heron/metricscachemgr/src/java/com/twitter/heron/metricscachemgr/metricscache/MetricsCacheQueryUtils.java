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
public final class MetricsCacheQueryUtils {
  // logger
  private static final Logger LOG = Logger.getLogger(MetricsCacheQueryUtils.class.getName());

  private MetricsCacheQueryUtils() {
  }

  /**
   * compatible with com.twitter.heron.proto.tmaster.TopologyMaster.MetricRequest
   */
  public static MetricRequest Convert(TopologyMaster.MetricRequest request) {
    String componentName = request.getComponentName();

    MetricRequest request1 = new MetricRequest();
    request1.componentNameInstanceId = new HashMap<>();
    if (request.getInstanceIdCount() == 0) {
      // empty list means all instances
      // 'null' means all instances
      request1.componentNameInstanceId.put(componentName, null);
    } else {
      Set<String> instances = new HashSet<>();
      // only one component
      request1.componentNameInstanceId.put(componentName, instances);
      // if there are instances specified
      instances.addAll(request.getInstanceIdList());
    }

    Set<String> metrics = new HashSet<>();
    request1.metricNames = metrics;
    if (request.getMetricCount() > 0) {
      metrics.addAll(request.getMetricList());
    } // empty list means no metrics

    // default: the whole time horizon
    request1.startTime = 0;
    request1.endTime = Long.MAX_VALUE;
    if (request.hasInterval()) { // endTime = now
      request1.endTime = System.currentTimeMillis();

      long interval = request.getInterval(); // in seconds
      if (interval <= 0) { // means all
        request1.startTime = 0;
      } else { // means [-interval, now]
        request1.startTime = request1.endTime - interval * 1000;
      }
    } else {
      request1.startTime = request.getExplicitInterval().getStart() * 1000;
      request1.endTime = request.getExplicitInterval().getEnd() * 1000;
    }

    // default: aggregate all metrics
    request1.minutely = 0;
    if (request.hasMinutely()) {
      request1.minutely = request.getMinutely() ? 1 : 0;
    }

    return request1;
  }

  /**
   * compatible with com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse
   */
  public static TopologyMaster.MetricResponse Convert(MetricResponse response,
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
      TopologyMaster.MetricResponse.TaskMetric.Builder builder1 =
          TopologyMaster.MetricResponse.TaskMetric.newBuilder();

      builder1.setInstanceId(instanceId);
      // add IndividualMetric
      for (String metricName : aggregation.get(instanceId).keySet()) {
        TopologyMaster.MetricResponse.IndividualMetric.Builder builder2 =
            TopologyMaster.MetricResponse.IndividualMetric.newBuilder();

        builder2.setName(metricName);
        // add value|IntervalValue
        List<MetricTimeRangeValue> list = aggregation.get(instanceId).get(metricName);
        if (list.size() == 1) {
          LOG.info("get0 " + list.get(0) + "; value " + list.get(0).value);
          builder2.setValue(list.get(0).value);
        } else {
          for (MetricTimeRangeValue v : list) {
            TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.Builder builder3 =
                TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.newBuilder();

            builder3.setValue(v.value);
            builder3.setInterval(TopologyMaster.MetricInterval.newBuilder()
                .setStart(v.startTime).setEnd(v.endTime));

            builder2.addIntervalValues(builder3);
          }// end IntervalValue
        }

        builder1.addMetric(builder2);
      }// end IndividualMetric

      builder.addMetric(builder1);
    }// end TaskMetric

    return builder.build();
  }

  // compatible with com.twitter.heron.proto.tmaster.TopologyMaster.ExceptionLogRequest
  public static ExceptionRequest Convert(TopologyMaster.ExceptionLogRequest request) {
    String componentName = request.getComponentName();

    ExceptionRequest request1 = new ExceptionRequest();
    request1.componentNameInstanceId = new HashMap<>();
    Set<String> instances = new HashSet<>();
    // only one component
    request1.componentNameInstanceId.put(componentName, instances);

    if (request.getInstancesCount() > 0) {
      instances.addAll(request.getInstancesList());
    } // empty list means all instances

    return request1;
  }

  // compatible with com.twitter.heron.proto.tmaster.TopologyMaster.ExceptionLogResponse
  public static TopologyMaster.ExceptionLogResponse Convert(ExceptionResponse response) {
    TopologyMaster.ExceptionLogResponse.Builder builder =
        TopologyMaster.ExceptionLogResponse.newBuilder();
    // default OK if we have response to build already
    builder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK));

    for (ExceptionDatapoint e : response.exceptionDatapointList) {
      TopologyMaster.TmasterExceptionLog.Builder builder1 =
          TopologyMaster.TmasterExceptionLog.newBuilder();
      // ExceptionDatapoint
      builder1.setComponentName(e.componentName);
      builder1.setHostname(e.hostname);
      builder1.setInstanceId(e.instanceId);
      // ExceptionData
      builder1.setStacktrace(e.stacktrace);
      builder1.setLasttime(e.lasttime);
      builder1.setFirsttime(e.firsttime);
      builder1.setCount(e.count);
      builder1.setLogging(e.logging);

      builder.addExceptions(builder1);
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
    // -1/0 means everything
    // What is the time interval that you want the metrics for
    // Clients can specify one in many ways.
    // 1. interval. Essentially seconds worth of metrics before the current time
    // 2. explicit_interval. An explicit interval
    public long startTime;
    public long endTime;
    // Do you want metrics broken down on a per minute basis?
    public int minutely;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{")
          .append("[").append(startTime).append("-").append(endTime)
          .append(":").append(minutely).append("]")
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
