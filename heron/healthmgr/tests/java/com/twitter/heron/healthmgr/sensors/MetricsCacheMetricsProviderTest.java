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

package com.twitter.heron.healthmgr.sensors;


import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.twitter.heron.proto.system.Common.Status;
import com.twitter.heron.proto.system.Common.StatusCode;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricInterval;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.IndividualMetric;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.IndividualMetric.IntervalValue;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.TaskMetric;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class MetricsCacheMetricsProviderTest {
  @Test
  public void provides1Comp2InstanceMetricsFromeMetricsCache() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("104")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481288)
                        .setEnd(1497481288)))))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_2")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("12")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481228)
                        .setEnd(1497481228)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("2")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481348)
                        .setEnd(1497481348)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("3")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481168)
                        .setEnd(1497481168)))))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Map<String, ComponentMetrics> metrics =
        spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp);

    assertEquals(1, metrics.size());
    assertNotNull(metrics.get(comp));
    assertEquals(2, metrics.get(comp).getMetrics().size());

    HashMap<String, InstanceMetrics> componentMetrics = metrics.get(comp).getMetrics();
    assertEquals(104,
        componentMetrics.get("container_1_bolt_1").getMetricValueSum(metric).intValue());
    assertEquals(17,
        componentMetrics.get("container_1_bolt_2").getMetricValueSum(metric).intValue());
  }

  @Test
  public void providesMultipleComponentMetricsFromMetricsCache() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp1 = "bolt-1";
    TopologyMaster.MetricResponse response1 = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt-1_2")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("104")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481288)
                        .setEnd(1497481288)))))
        .build();

    doReturn(response1).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp1, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    String comp2 = "bolt-2";
    TopologyMaster.MetricResponse response2 = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt-2_1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("12")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481228)
                        .setEnd(1497481228)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("2")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481348)
                        .setEnd(1497481348)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("3")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481168)
                        .setEnd(1497481168)))))
        .build();

    doReturn(response2).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp2, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Map<String, ComponentMetrics> metrics
        = spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp1, comp2);

    assertEquals(2, metrics.size());
    assertNotNull(metrics.get(comp1));
    assertEquals(1, metrics.get(comp1).getMetrics().size());
    assertEquals(104,
        metrics.get(comp1).getMetricValueSum("container_1_bolt-1_2", metric).intValue());

    assertNotNull(metrics.get(comp2));
    assertEquals(1, metrics.get(comp2).getMetrics().size());
    assertEquals(17,
        metrics.get(comp2).getMetricValueSum("container_1_bolt-2_1", metric).intValue());
  }

  @Test
  public void parsesBackPressureMetric() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "__time_spent_back_pressure_by_compid/container_1_split_1";
    String comp = "__stmgr__";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("stmgr-1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("601")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(0)
                        .setEnd(0)))))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));
    Map<String, ComponentMetrics> metrics
        = spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp);

    assertEquals(1, metrics.size());
    assertNotNull(metrics.get(comp));
    assertEquals(1, metrics.get(comp).getMetrics().size());

    HashMap<String, InstanceMetrics> componentMetrics = metrics.get(comp).getMetrics();
    assertEquals(601, componentMetrics.get("stmgr-1").getMetricValueSum(metric).intValue());
  }

  @Test
  public void handleMissingData() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "dummy";
    String comp = "split";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));
    Map<String, ComponentMetrics> metrics
        = spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp);

    assertEquals(1, metrics.size());
    assertNotNull(metrics.get(comp));
    assertEquals(0, metrics.get(comp).getMetrics().size());
  }

  private MetricsCacheMetricsProvider createMetricsProviderSpy() {
    MetricsCacheMetricsProvider metricsProvider
        = new MetricsCacheMetricsProvider("localhost");

    MetricsCacheMetricsProvider spyMetricsProvider = spy(metricsProvider);
    spyMetricsProvider.setClock(new TestClock(70000));
    return spyMetricsProvider;
  }

  @Test
  public void testGetTimeLineMetrics() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("104")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481288)
                        .setEnd(1497481288)))))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_2")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("12")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481228)
                        .setEnd(1497481228)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("2")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481348)
                        .setEnd(1497481348)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("3")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481168)
                        .setEnd(1497481168)))))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Map<String, ComponentMetrics> metrics =
        spyMetricsProvider
            .getComponentMetrics(metric, Instant.ofEpochSecond(10), Duration.ofSeconds(60), comp);

    assertEquals(1, metrics.size());
    ComponentMetrics componentMetrics = metrics.get(comp);
    assertNotNull(componentMetrics);
    assertEquals(2, componentMetrics.getMetrics().size());

    InstanceMetrics instanceMetrics = componentMetrics.getMetrics("container_1_bolt_1");
    assertNotNull(instanceMetrics);
    assertEquals(1, instanceMetrics.getMetrics().size());

    Map<Instant, Double> metricValues = instanceMetrics.getMetrics().get(metric);
    assertEquals(1, metricValues.size());
    assertEquals(104, metricValues.get(Instant.ofEpochSecond(1497481288)).intValue());

    instanceMetrics = componentMetrics.getMetrics("container_1_bolt_2");
    assertNotNull(instanceMetrics);
    assertEquals(1, instanceMetrics.getMetrics().size());

    metricValues = instanceMetrics.getMetrics().get(metric);
    assertEquals(3, metricValues.size());
    assertEquals(12, metricValues.get(Instant.ofEpochSecond(1497481228L)).intValue());
    assertEquals(2, metricValues.get(Instant.ofEpochSecond(1497481348L)).intValue());
    assertEquals(3, metricValues.get(Instant.ofEpochSecond(1497481168L)).intValue());
  }

  private class TestClock extends MetricsCacheMetricsProvider.Clock {
    long timeStamp;

    TestClock(long timeStamp) {
      this.timeStamp = timeStamp;
    }

    @Override
    long currentTime() {
      return timeStamp;
    }
  }
}
