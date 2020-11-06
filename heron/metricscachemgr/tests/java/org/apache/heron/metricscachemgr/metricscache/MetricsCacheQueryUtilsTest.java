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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

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

import static org.apache.heron.metricscachemgr.metricscache.MetricsCacheQueryUtils.toProtobuf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetricsCacheQueryUtilsTest {
  private static void assertMetricRequest(
      MetricRequest request,
      List<AbstractMap.SimpleEntry<String, List<String>>> componentNameInstanceId,
      List<String> metricNames, MetricGranularity aggregationGranularity) {
    assertEquals(aggregationGranularity, request.getAggregationGranularity());

    Set<String> actualMetricName = request.getMetricNames();
    if (metricNames == null) {
      assertNull(actualMetricName);
    } else {
      assertEquals(metricNames.size(), actualMetricName.size());
      for (String name : metricNames) {
        assertTrue(actualMetricName.contains(name));
      }
    }

    Map<String, Set<String>> actualComponentInstance = request.getComponentNameInstanceId();
    if (componentNameInstanceId == null) {
      assertNull(actualComponentInstance);
    } else {
      assertEquals(componentNameInstanceId.size(), actualComponentInstance.size());
      for (Map.Entry<String, List<String>> entry : componentNameInstanceId) {
        assertTrue(actualComponentInstance.containsKey(entry.getKey()));

        Set<String> actualInstances = actualComponentInstance.get(entry.getKey());
        if (actualInstances == null) {
          assertNull(entry.getValue());
        }
        assertEquals(entry.getValue().size(), actualInstances.size());
        for (String ins : entry.getValue()) {
          assertTrue(actualInstances.contains(ins));
        }
      }
    }
  }

  @Test
  public void testFromProtoBufMetricInterval() {
    TopologyManager.MetricRequest request =
        TopologyManager.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .addMetric("m1").addMetric("m2")
            .setInterval(100)// in seconds
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertMetricRequest(request1,
        Arrays.asList(
            new AbstractMap.SimpleEntry<String, List<String>>("c1", Arrays.asList("i1", "i2"))),
        Arrays.asList("m1", "m2"),
        MetricGranularity.AGGREGATE_ALL_METRICS);
    // in milli-seconds
    assertEquals(100 * 1000, request1.getEndTime() - request1.getStartTime());
  }

  @Test
  public void testFromProtoBufMetricExplicitInterval() {
    TopologyManager.MetricRequest request =
        TopologyManager.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .addMetric("m1").addMetric("m2")
            .setExplicitInterval(TopologyManager.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertMetricRequest(request1,
        Arrays.asList(
            new AbstractMap.SimpleEntry<String, List<String>>("c1", Arrays.asList("i1", "i2"))),
        Arrays.asList("m1", "m2"),
        MetricGranularity.AGGREGATE_ALL_METRICS);
    assertEquals(100 * 1000, request1.getStartTime()); // in milli-seconds
    assertEquals(200 * 1000, request1.getEndTime()); // in milli-seconds
  }

  @Test
  public void testFromProtoBufMetricMinutely() {
    TopologyManager.MetricRequest request =
        TopologyManager.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .addMetric("m1").addMetric("m2")
            .setExplicitInterval(TopologyManager.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .setMinutely(true)
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertMetricRequest(request1,
        Arrays.asList(
            new AbstractMap.SimpleEntry<String, List<String>>("c1", Arrays.asList("i1", "i2"))),
        Arrays.asList("m1", "m2"),
        MetricGranularity.AGGREGATE_BY_BUCKET);
    assertEquals(100 * 1000, request1.getStartTime()); // in milli-seconds
    assertEquals(200 * 1000, request1.getEndTime()); // in milli-seconds
  }

  @Test
  public void testFromProtoBufMetricEmptyMetrics() {
    TopologyManager.MetricRequest request =
        TopologyManager.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .setExplicitInterval(TopologyManager.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .setMinutely(true)
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertMetricRequest(request1,
        Arrays.asList(
            new AbstractMap.SimpleEntry<String, List<String>>("c1", Arrays.asList("i1", "i2"))),
        Arrays.asList(new String[]{}),
        MetricGranularity.AGGREGATE_BY_BUCKET);
    assertEquals(100 * 1000, request1.getStartTime()); // in milli-seconds
    assertEquals(200 * 1000, request1.getEndTime()); // in milli-seconds
  }

  @Test
  public void testFromProtoBufMetricEmptyInstanceIds() {
    TopologyManager.MetricRequest request =
        TopologyManager.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addMetric("m1").addMetric("m2")
            .setExplicitInterval(TopologyManager.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .setMinutely(true)
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertEquals(null, request1.getComponentNameInstanceId().get("c1"));
  }

  @Test
  public void testToProtoBufMetric() {
    List<MetricTimeRangeValue> list = new ArrayList<>();
    list.add(new MetricTimeRangeValue(300 * 1000, 400 * 1000, "v1"));
    MetricResponse response = new MetricResponse(
        Arrays.asList(new MetricDatum("c1", "i1", "m1", list)));

    long startTime = 100 * 1000;
    long endTime = 200 * 1000;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, null);

    TopologyManager.MetricResponse response1 = toProtobuf(response, request);

    assertEquals(200 - 100, response1.getInterval());
    assertEquals(Common.StatusCode.OK, response1.getStatus().getStatus());
    assertEquals(1, response1.getMetricCount());
    assertEquals("i1", response1.getMetric(0).getInstanceId());
    assertEquals(1, response1.getMetric(0).getMetricCount());
    assertEquals("m1", response1.getMetric(0).getMetric(0).getName());
    assertEquals("v1", response1.getMetric(0).getMetric(0).getValue());
  }

  @Test
  public void testToProtoBufMetric2() {
    List<MetricTimeRangeValue> list = new ArrayList<>();
    list.add(new MetricTimeRangeValue(300 * 1000, 400 * 1000, "v1"));
    list.add(new MetricTimeRangeValue(500 * 1000, 600 * 1000, "v2"));
    MetricResponse response = new MetricResponse(
        Arrays.asList(new MetricDatum("c1", "i1", "m1", list)));

    long startTime = 100 * 1000;
    long endTime = 200 * 1000;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, null);

    TopologyManager.MetricResponse response1 = toProtobuf(response, request);

    assertEquals(200 - 100, response1.getInterval());
    assertEquals(Common.StatusCode.OK, response1.getStatus().getStatus());
    assertEquals(1, response1.getMetricCount());
    assertEquals("i1", response1.getMetric(0).getInstanceId());
    assertEquals(1, response1.getMetric(0).getMetricCount());
    assertEquals("m1", response1.getMetric(0).getMetric(0).getName());
    assertEquals(2, response1.getMetric(0).getMetric(0).getIntervalValuesCount());
    assertEquals("v1", response1.getMetric(0).getMetric(0).getIntervalValues(0).getValue());
    assertEquals(300 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(0).getInterval().getStart());
    assertEquals(400 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(0).getInterval().getEnd());
    assertEquals("v2", response1.getMetric(0).getMetric(0).getIntervalValues(1).getValue());
    assertEquals(500 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(1).getInterval().getStart());
    assertEquals(600 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(1).getInterval().getEnd());
  }

  @Test
  public void testFromProtoBufException() {
    TopologyManager.ExceptionLogRequest request =
        TopologyManager.ExceptionLogRequest.newBuilder()
            .setComponentName("c1")
            .addInstances("i1").addInstances("i2")
            .build();

    ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    Map<String, Set<String>> componentNameInstanceId = request1.getComponentNameInstanceId();

    assertEquals(1, componentNameInstanceId.keySet().size());
    assertEquals(true, componentNameInstanceId.containsKey("c1"));
    assertEquals(2, componentNameInstanceId.get("c1").size());
    assertEquals(true, componentNameInstanceId.get("c1").contains("i1"));
    assertEquals(true, componentNameInstanceId.get("c1").contains("i2"));
  }

  @Test
  public void testFromProtoBufExceptionEmptyInstances() {
    TopologyManager.ExceptionLogRequest request =
        TopologyManager.ExceptionLogRequest.newBuilder()
            .setComponentName("c1")
            .build();

    ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);
    Map<String, Set<String>> componentNameInstanceId = request1.getComponentNameInstanceId();

    assertEquals(1, componentNameInstanceId.keySet().size());
    assertEquals(true, componentNameInstanceId.containsKey("c1"));
    assertEquals(null, componentNameInstanceId.get("c1"));
  }

  @Test
  public void testToProtoBufException() {
    List<ExceptionDatum> response = new ArrayList<>();
    ExceptionDatum dp = new ExceptionDatum("c1", "i1", "h1", "s1", "lt1", "ft1", 10, "l1");
    response.add(dp);

    TopologyManager.ExceptionLogResponse response1 = toProtobuf(new ExceptionResponse(response));

    assertEquals(1, response1.getExceptionsCount());
    assertEquals("c1", response1.getExceptions(0).getComponentName());
    assertEquals("i1", response1.getExceptions(0).getInstanceId());
    assertEquals("h1", response1.getExceptions(0).getHostname());
    assertEquals("s1", response1.getExceptions(0).getStacktrace());
    assertEquals("l1", response1.getExceptions(0).getLogging());
    assertEquals("ft1", response1.getExceptions(0).getFirsttime());
    assertEquals("lt1", response1.getExceptions(0).getLasttime());
    assertEquals(10, response1.getExceptions(0).getCount());
  }
}
