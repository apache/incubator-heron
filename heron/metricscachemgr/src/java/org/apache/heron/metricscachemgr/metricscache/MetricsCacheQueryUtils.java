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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.heron.metricscachemgr.metricscache.query.ExceptionDatum;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionRequest;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionResponse;
import org.apache.heron.metricscachemgr.metricscache.query.MetricDatum;
import org.apache.heron.metricscachemgr.metricscache.query.MetricGranularity;
import org.apache.heron.metricscachemgr.metricscache.query.MetricRequest;
import org.apache.heron.metricscachemgr.metricscache.query.MetricResponse;
import org.apache.heron.metricscachemgr.metricscache.query.MetricTimeRangeValue;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.tmanager.TopologyManager;

import static org.apache.heron.metricscachemgr.metricscache.query.MetricGranularity.AGGREGATE_ALL_METRICS;
import static org.apache.heron.metricscachemgr.metricscache.query.MetricGranularity.AGGREGATE_BY_BUCKET;

/**
 * converter from/to protobuf
 */
public final class MetricsCacheQueryUtils {
  private MetricsCacheQueryUtils() {
  }

  /**
   * compatible with org.apache.heron.proto.tmanager.TopologyManager.MetricRequest
   * @param request protobuf defined message
   * @return metricscache defined data structure
   */
  public static MetricRequest fromProtobuf(TopologyManager.MetricRequest request) {
    String componentName = request.getComponentName();

    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    if (request.getInstanceIdCount() == 0) {
      // empty list means all instances
      // 'null' means all instances
      componentNameInstanceId.put(componentName, null);
    } else {
      Set<String> instances = new HashSet<>();
      // only one component
      componentNameInstanceId.put(componentName, instances);
      // if there are instances specified
      instances.addAll(request.getInstanceIdList());
    }

    Set<String> metricNames = new HashSet<>();
    if (request.getMetricCount() > 0) {
      metricNames.addAll(request.getMetricList());
    } // empty list means no metrics

    // default: the whole time horizon
    long startTime = 0;
    long endTime = Long.MAX_VALUE;
    if (request.hasInterval()) { // endTime = now
      endTime = System.currentTimeMillis();

      long interval = request.getInterval(); // in seconds
      if (interval <= 0) { // means all
        startTime = 0;
      } else { // means [-interval, now]
        startTime = endTime - interval * 1000;
      }
    } else {
      startTime = request.getExplicitInterval().getStart() * 1000;
      endTime = request.getExplicitInterval().getEnd() * 1000;
    }

    // default: aggregate all metrics
    MetricGranularity aggregationGranularity = AGGREGATE_ALL_METRICS;
    if (request.hasMinutely() && request.getMinutely()) {
      aggregationGranularity = AGGREGATE_BY_BUCKET;
    }

    return new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, aggregationGranularity);
  }

  /**
   * compatible with org.apache.heron.proto.tmanager.TopologyManager.MetricResponse
   * @param response metricscache defined data structure
   * @param request metricscache defined data structure
   * @return protobuf defined message
   */
  public static TopologyManager.MetricResponse toProtobuf(MetricResponse response,
                                                         MetricRequest request) {
    TopologyManager.MetricResponse.Builder builder =
        TopologyManager.MetricResponse.newBuilder();
    builder.setInterval((request.getEndTime() - request.getStartTime()) / 1000); // in seconds

    // default OK if we have response to build already
    builder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK));

    // instanceId -> [metricName -> metricValue]
    // componentName is ignored, since there is only one component in the query
    Map<String, Map<String, List<MetricTimeRangeValue>>> aggregation =
        new HashMap<>();
    for (MetricDatum datum : response.getMetricList()) {
      String instanceId = datum.getInstanceId();
      String metricName = datum.getMetricName();
      List<MetricTimeRangeValue> metricValue = datum.getMetricValue();
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
      TopologyManager.MetricResponse.TaskMetric.Builder taskMetricBuilder =
          TopologyManager.MetricResponse.TaskMetric.newBuilder();

      taskMetricBuilder.setInstanceId(instanceId);
      // add IndividualMetric
      for (String metricName : aggregation.get(instanceId).keySet()) {
        TopologyManager.MetricResponse.IndividualMetric.Builder individualMetricBuilder =
            TopologyManager.MetricResponse.IndividualMetric.newBuilder();

        individualMetricBuilder.setName(metricName);
        // add value|IntervalValue
        List<MetricTimeRangeValue> list = aggregation.get(instanceId).get(metricName);
        if (list.size() == 1) {
          individualMetricBuilder.setValue(list.get(0).getValue());
        } else {
          for (MetricTimeRangeValue v : list) {
            TopologyManager.MetricResponse.IndividualMetric.IntervalValue.Builder
                intervalValueBuilder =
                TopologyManager.MetricResponse.IndividualMetric.IntervalValue.newBuilder();

            intervalValueBuilder.setValue(v.getValue());
            intervalValueBuilder.setInterval(TopologyManager.MetricInterval.newBuilder()
                .setStart(v.getStartTime()).setEnd(v.getEndTime()));

            individualMetricBuilder.addIntervalValues(intervalValueBuilder);
          }// end IntervalValue
        }

        taskMetricBuilder.addMetric(individualMetricBuilder);
      }// end IndividualMetric

      builder.addMetric(taskMetricBuilder);
    }// end TaskMetric

    return builder.build();
  }

  // compatible with org.apache.heron.proto.tmanager.TopologyManager.ExceptionLogRequest
  public static ExceptionRequest fromProtobuf(TopologyManager.ExceptionLogRequest request) {
    String componentName = request.getComponentName();

    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    Set<String> instances = null;

    if (request.getInstancesCount() > 0) {
      instances = new HashSet<>();
      instances.addAll(request.getInstancesList());
    } // empty list means all instances; 'null' means all instances

    // only one component
    componentNameInstanceId.put(componentName, instances);

    return new ExceptionRequest(componentNameInstanceId);
  }

  // compatible with org.apache.heron.proto.tmanager.TopologyManager.ExceptionLogResponse
  public static TopologyManager.ExceptionLogResponse toProtobuf(ExceptionResponse response) {
    TopologyManager.ExceptionLogResponse.Builder builder =
        TopologyManager.ExceptionLogResponse.newBuilder();
    // default OK if we have response to build already
    builder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK));

    for (ExceptionDatum e : response.getExceptionDatapointList()) {
      TopologyManager.TmanagerExceptionLog.Builder exceptionBuilder =
          TopologyManager.TmanagerExceptionLog.newBuilder();
      // ExceptionDatapoint
      exceptionBuilder.setComponentName(e.getComponentName());
      exceptionBuilder.setHostname(e.getHostname());
      exceptionBuilder.setInstanceId(e.getInstanceId());
      // ExceptionData
      exceptionBuilder.setStacktrace(e.getStackTrace());
      exceptionBuilder.setLasttime(e.getLastTime());
      exceptionBuilder.setFirsttime(e.getFirstTime());
      exceptionBuilder.setCount(e.getCount());
      exceptionBuilder.setLogging(e.getLogging());

      builder.addExceptions(exceptionBuilder);
    }

    return builder.build();
  }
}
