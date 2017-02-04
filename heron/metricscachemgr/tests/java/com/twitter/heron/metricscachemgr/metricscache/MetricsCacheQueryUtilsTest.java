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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.metricscachemgr.metricscache.query.ExceptionDatum;
import com.twitter.heron.metricscachemgr.metricscache.query.ExceptionRequest;
import com.twitter.heron.metricscachemgr.metricscache.query.ExceptionResponse;
import com.twitter.heron.metricscachemgr.metricscache.query.MetricDatum;
import com.twitter.heron.metricscachemgr.metricscache.query.MetricGranularity;
import com.twitter.heron.metricscachemgr.metricscache.query.MetricRequest;
import com.twitter.heron.metricscachemgr.metricscache.query.MetricResponse;
import com.twitter.heron.metricscachemgr.metricscache.query.MetricTimeRangeValue;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;

import static com.twitter.heron.metricscachemgr.metricscache.MetricsCacheQueryUtils.toProtobuf;

public class MetricsCacheQueryUtilsTest {
  private static void assertMetricRequest(
      MetricRequest request,
      List<AbstractMap.SimpleEntry<String, List<String>>> componentNameInstanceId,
      List<String> metricNames, MetricGranularity aggregationGranularity) {
    Assert.assertEquals(aggregationGranularity, request.getAggregationGranularity());

    Set<String> actualMetricName = request.getMetricNames();
    if (metricNames == null) {
      Assert.assertNull(actualMetricName);
    } else {
      Assert.assertEquals(metricNames.size(), actualMetricName.size());
      for (String name : metricNames) {
        Assert.assertTrue(actualMetricName.contains(name));
      }
    }

    Map<String, Set<String>> actualComponentInstance = request.getComponentNameInstanceId();
    if (componentNameInstanceId == null) {
      Assert.assertNull(actualComponentInstance);
    } else {
      Assert.assertEquals(componentNameInstanceId.size(), actualComponentInstance.size());
      for (Map.Entry<String, List<String>> entry : componentNameInstanceId) {
        Assert.assertTrue(actualComponentInstance.containsKey(entry.getKey()));

        Set<String> actualInstances = actualComponentInstance.get(entry.getKey());
        if (actualInstances == null) {
          Assert.assertNull(entry.getValue());
        }
        Assert.assertEquals(entry.getValue().size(), actualInstances.size());
        for (String ins : entry.getValue()) {
          Assert.assertTrue(actualInstances.contains(ins));
        }
      }
    }
  }

  @Test
  public void testFromProtoBufMetricInterval() {
    TopologyMaster.MetricRequest request =
        TopologyMaster.MetricRequest.newBuilder()
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
    Assert.assertEquals(100 * 1000, request1.getEndTime() - request1.getStartTime());
  }

  @Test
  public void testFromProtoBufMetricExplicitInterval() {
    TopologyMaster.MetricRequest request =
        TopologyMaster.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .addMetric("m1").addMetric("m2")
            .setExplicitInterval(TopologyMaster.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertMetricRequest(request1,
        Arrays.asList(
            new AbstractMap.SimpleEntry<String, List<String>>("c1", Arrays.asList("i1", "i2"))),
        Arrays.asList("m1", "m2"),
        MetricGranularity.AGGREGATE_ALL_METRICS);
    Assert.assertEquals(100 * 1000, request1.getStartTime()); // in milli-seconds
    Assert.assertEquals(200 * 1000, request1.getEndTime()); // in milli-seconds
  }

  @Test
  public void testFromProtoBufMetricMinutely() {
    TopologyMaster.MetricRequest request =
        TopologyMaster.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .addMetric("m1").addMetric("m2")
            .setExplicitInterval(TopologyMaster.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .setMinutely(true)
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertMetricRequest(request1,
        Arrays.asList(
            new AbstractMap.SimpleEntry<String, List<String>>("c1", Arrays.asList("i1", "i2"))),
        Arrays.asList("m1", "m2"),
        MetricGranularity.AGGREGATE_BY_BUCKET);
    Assert.assertEquals(100 * 1000, request1.getStartTime()); // in milli-seconds
    Assert.assertEquals(200 * 1000, request1.getEndTime()); // in milli-seconds
  }

  @Test
  public void testFromProtoBufMetricEmptyMetrics() {
    TopologyMaster.MetricRequest request =
        TopologyMaster.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .setExplicitInterval(TopologyMaster.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .setMinutely(true)
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    assertMetricRequest(request1,
        Arrays.asList(
            new AbstractMap.SimpleEntry<String, List<String>>("c1", Arrays.asList("i1", "i2"))),
        Arrays.asList(new String[]{}),
        MetricGranularity.AGGREGATE_BY_BUCKET);
    Assert.assertEquals(100 * 1000, request1.getStartTime()); // in milli-seconds
    Assert.assertEquals(200 * 1000, request1.getEndTime()); // in milli-seconds
  }

  @Test
  public void testFromProtoBufMetricEmptyInstanceIds() {
    TopologyMaster.MetricRequest request =
        TopologyMaster.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addMetric("m1").addMetric("m2")
            .setExplicitInterval(TopologyMaster.MetricInterval.newBuilder()
                .setStart(100).setEnd(200)) // in seconds
            .setMinutely(true)
            .build();

    MetricRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(null, request1.getComponentNameInstanceId().get("c1"));
  }

  @Test
  public void testToProtoBufMetric() {
    List<MetricTimeRangeValue> list = new ArrayList<>();
    list.add(new MetricTimeRangeValue(300 * 1000, 400 * 1000, "v1"));
    MetricResponse response = new MetricResponse();
    response.metricList = new ArrayList<>();
    response.metricList.add(new MetricDatum("c1", "i1", "m1", list));

    long startTime = 100 * 1000;
    long endTime = 200 * 1000;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, null);

    TopologyMaster.MetricResponse response1 = toProtobuf(response, request);

    Assert.assertEquals(200 - 100, response1.getInterval());
    Assert.assertEquals(Common.StatusCode.OK, response1.getStatus().getStatus());
    Assert.assertEquals(1, response1.getMetricCount());
    Assert.assertEquals("i1", response1.getMetric(0).getInstanceId());
    Assert.assertEquals(1, response1.getMetric(0).getMetricCount());
    Assert.assertEquals("m1", response1.getMetric(0).getMetric(0).getName());
    Assert.assertEquals("v1", response1.getMetric(0).getMetric(0).getValue());
  }

  @Test
  public void testToProtoBufMetric2() {
    List<MetricTimeRangeValue> list = new ArrayList<>();
    list.add(new MetricTimeRangeValue(300 * 1000, 400 * 1000, "v1"));
    list.add(new MetricTimeRangeValue(500 * 1000, 600 * 1000, "v2"));
    MetricResponse response = new MetricResponse();
    response.metricList = new ArrayList<>();
    response.metricList.add(new MetricDatum("c1", "i1", "m1", list));

    long startTime = 100 * 1000;
    long endTime = 200 * 1000;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, null);

    TopologyMaster.MetricResponse response1 = toProtobuf(response, request);

    Assert.assertEquals(200 - 100, response1.getInterval());
    Assert.assertEquals(Common.StatusCode.OK, response1.getStatus().getStatus());
    Assert.assertEquals(1, response1.getMetricCount());
    Assert.assertEquals("i1", response1.getMetric(0).getInstanceId());
    Assert.assertEquals(1, response1.getMetric(0).getMetricCount());
    Assert.assertEquals("m1", response1.getMetric(0).getMetric(0).getName());
    Assert.assertEquals(2, response1.getMetric(0).getMetric(0).getIntervalValuesCount());
    Assert.assertEquals("v1", response1.getMetric(0).getMetric(0).getIntervalValues(0).getValue());
    Assert.assertEquals(300 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(0).getInterval().getStart());
    Assert.assertEquals(400 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(0).getInterval().getEnd());
    Assert.assertEquals("v2", response1.getMetric(0).getMetric(0).getIntervalValues(1).getValue());
    Assert.assertEquals(500 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(1).getInterval().getStart());
    Assert.assertEquals(600 * 1000,
        response1.getMetric(0).getMetric(0).getIntervalValues(1).getInterval().getEnd());
  }

  @Test
  public void testFromProtoBufException() {
    TopologyMaster.ExceptionLogRequest request =
        TopologyMaster.ExceptionLogRequest.newBuilder()
            .setComponentName("c1")
            .addInstances("i1").addInstances("i2")
            .build();

    ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true, request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(2, request1.componentNameInstanceId.get("c1").size());
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i1"));
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i2"));
  }

  @Test
  public void testFromProtoBufExceptionEmptyInstances() {
    TopologyMaster.ExceptionLogRequest request =
        TopologyMaster.ExceptionLogRequest.newBuilder()
            .setComponentName("c1")
            .build();

    ExceptionRequest request1 = MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true, request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(null, request1.componentNameInstanceId.get("c1"));
  }

  @Test
  public void testToProtoBufException() {
    ExceptionResponse response = new ExceptionResponse();
    response.exceptionDatapointList = new ArrayList<>();
    ExceptionDatum dp = new ExceptionDatum("c1", "i1", "h1", "s1", "lt1", "ft1", 10, "l1");
    response.exceptionDatapointList.add(dp);

    TopologyMaster.ExceptionLogResponse response1 = toProtobuf(response);

    Assert.assertEquals(1, response1.getExceptionsCount());
    Assert.assertEquals("c1", response1.getExceptions(0).getComponentName());
    Assert.assertEquals("i1", response1.getExceptions(0).getInstanceId());
    Assert.assertEquals("h1", response1.getExceptions(0).getHostname());
    Assert.assertEquals("s1", response1.getExceptions(0).getStacktrace());
    Assert.assertEquals("l1", response1.getExceptions(0).getLogging());
    Assert.assertEquals("ft1", response1.getExceptions(0).getFirsttime());
    Assert.assertEquals("lt1", response1.getExceptions(0).getLasttime());
    Assert.assertEquals(10, response1.getExceptions(0).getCount());
  }
}
