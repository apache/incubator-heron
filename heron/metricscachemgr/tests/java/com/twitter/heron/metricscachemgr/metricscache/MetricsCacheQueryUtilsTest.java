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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.metricscachemgr.metricscache.datapoint.ExceptionDatapoint;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;

import static com.twitter.heron.metricscachemgr.metricscache.MetricsCacheQueryUtils.toProtobuf;

public class MetricsCacheQueryUtilsTest {
  @Test
  public void testFromProtoBufMetricInterval() {
    TopologyMaster.MetricRequest request =
        TopologyMaster.MetricRequest.newBuilder()
            .setComponentName("c1")
            .addInstanceId("i1").addInstanceId("i2")
            .addMetric("m1").addMetric("m2")
            .setInterval(100)// in seconds
            .build();

    MetricsCacheQueryUtils.MetricRequest request1 =
        MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true, request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(2, request1.componentNameInstanceId.get("c1").size());
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i1"));
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i2"));
    Assert.assertEquals(100 * 1000, request1.endTime - request1.startTime); // in milli-seconds
    Assert.assertEquals(MetricsCacheQueryUtils.Granularity.AGGREGATE_ALL_METRICS,
        request1.aggregationGranularity);
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

    MetricsCacheQueryUtils.MetricRequest request1 =
        MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true, request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(2, request1.componentNameInstanceId.get("c1").size());
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i1"));
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i2"));
    Assert.assertEquals(2, request1.metricNames.size());
    Assert.assertEquals(true, request1.metricNames.contains("m1"));
    Assert.assertEquals(true, request1.metricNames.contains("m2"));
    Assert.assertEquals(100 * 1000, request1.startTime); // in milli-seconds
    Assert.assertEquals(200 * 1000, request1.endTime); // in milli-seconds
    Assert.assertEquals(MetricsCacheQueryUtils.Granularity.AGGREGATE_ALL_METRICS,
        request1.aggregationGranularity);
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

    MetricsCacheQueryUtils.MetricRequest request1 =
        MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true, request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(2, request1.componentNameInstanceId.get("c1").size());
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i1"));
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i2"));
    Assert.assertEquals(2, request1.metricNames.size());
    Assert.assertEquals(true, request1.metricNames.contains("m1"));
    Assert.assertEquals(true, request1.metricNames.contains("m2"));
    Assert.assertEquals(100 * 1000, request1.startTime); // in milli-seconds
    Assert.assertEquals(200 * 1000, request1.endTime); // in milli-seconds
    Assert.assertEquals(MetricsCacheQueryUtils.Granularity.AGGREGATE_BY_BUCKET,
        request1.aggregationGranularity);
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

    MetricsCacheQueryUtils.MetricRequest request1 =
        MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true, request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(2, request1.componentNameInstanceId.get("c1").size());
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i1"));
    Assert.assertEquals(true, request1.componentNameInstanceId.get("c1").contains("i2"));
    Assert.assertEquals(0, request1.metricNames.size());
    Assert.assertEquals(100 * 1000, request1.startTime); // in milli-seconds
    Assert.assertEquals(200 * 1000, request1.endTime); // in milli-seconds
    Assert.assertEquals(MetricsCacheQueryUtils.Granularity.AGGREGATE_BY_BUCKET,
        request1.aggregationGranularity);
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

    MetricsCacheQueryUtils.MetricRequest request1 =
        MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true,
        request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(null, request1.componentNameInstanceId.get("c1"));
    Assert.assertEquals(2, request1.metricNames.size());
    Assert.assertEquals(true, request1.metricNames.contains("m1"));
    Assert.assertEquals(true, request1.metricNames.contains("m2"));
    Assert.assertEquals(100 * 1000, request1.startTime); // in milli-seconds
    Assert.assertEquals(200 * 1000, request1.endTime); // in milli-seconds
    Assert.assertEquals(MetricsCacheQueryUtils.Granularity.AGGREGATE_BY_BUCKET,
        request1.aggregationGranularity);
  }

  @Test
  public void testToProtoBufMetric() {
    List<MetricsCacheQueryUtils.MetricTimeRangeValue> list = new ArrayList<>();
    list.add(new MetricsCacheQueryUtils.MetricTimeRangeValue(300 * 1000, 400 * 1000, "v1"));
    MetricsCacheQueryUtils.MetricResponse response = new MetricsCacheQueryUtils.MetricResponse();
    response.metricList = new ArrayList<>();
    response.metricList.add(new MetricsCacheQueryUtils.MetricDatum("c1", "i1", "m1", list));

    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = 100 * 1000;
    request.endTime = 200 * 1000;

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
    List<MetricsCacheQueryUtils.MetricTimeRangeValue> list = new ArrayList<>();
    list.add(new MetricsCacheQueryUtils.MetricTimeRangeValue(300 * 1000, 400 * 1000, "v1"));
    list.add(new MetricsCacheQueryUtils.MetricTimeRangeValue(500 * 1000, 600 * 1000, "v2"));
    MetricsCacheQueryUtils.MetricResponse response = new MetricsCacheQueryUtils.MetricResponse();
    response.metricList = new ArrayList<>();
    response.metricList.add(new MetricsCacheQueryUtils.MetricDatum("c1", "i1", "m1", list));

    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = 100 * 1000;
    request.endTime = 200 * 1000;

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

    MetricsCacheQueryUtils.ExceptionRequest request1 =
        MetricsCacheQueryUtils.fromProtobuf(request);

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

    MetricsCacheQueryUtils.ExceptionRequest request1 =
        MetricsCacheQueryUtils.fromProtobuf(request);

    Assert.assertEquals(1, request1.componentNameInstanceId.keySet().size());
    Assert.assertEquals(true, request1.componentNameInstanceId.containsKey("c1"));
    Assert.assertEquals(null, request1.componentNameInstanceId.get("c1"));
  }

  @Test
  public void testToProtoBufException() {
    MetricsCacheQueryUtils.ExceptionResponse response =
        new MetricsCacheQueryUtils.ExceptionResponse();
    response.exceptionDatapointList = new ArrayList<>();
    ExceptionDatapoint dp = new ExceptionDatapoint();
    dp.componentName = "c1";
    dp.instanceId = "i1";
    dp.hostname = "h1";
    dp.stackTrace = "s1";
    dp.firstTime = "100";
    dp.lastTime = "200";
    dp.count = 10;
    dp.logging = "l1";
    response.exceptionDatapointList.add(dp);

    TopologyMaster.ExceptionLogResponse response1 = toProtobuf(response);

    Assert.assertEquals(1, response1.getExceptionsCount());
    Assert.assertEquals("c1", response1.getExceptions(0).getComponentName());
    Assert.assertEquals("i1", response1.getExceptions(0).getInstanceId());
    Assert.assertEquals("h1", response1.getExceptions(0).getHostname());
    Assert.assertEquals("s1", response1.getExceptions(0).getStacktrace());
    Assert.assertEquals("l1", response1.getExceptions(0).getLogging());
    Assert.assertEquals("100", response1.getExceptions(0).getFirsttime());
    Assert.assertEquals("200", response1.getExceptions(0).getLasttime());
    Assert.assertEquals(10, response1.getExceptions(0).getCount());
  }
}
