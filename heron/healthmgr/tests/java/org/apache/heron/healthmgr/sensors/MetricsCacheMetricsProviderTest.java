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

package org.apache.heron.healthmgr.sensors;


import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;

import org.junit.Test;
import org.mockito.Mockito;



import org.apache.heron.proto.system.Common.Status;
import org.apache.heron.proto.system.Common.StatusCode;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.proto.tmanager.TopologyManager.MetricInterval;
import org.apache.heron.proto.tmanager.TopologyManager.MetricResponse.IndividualMetric;
import org.apache.heron.proto.tmanager.TopologyManager.MetricResponse.IndividualMetric.IntervalValue;
import org.apache.heron.proto.tmanager.TopologyManager.MetricResponse.TaskMetric;
import org.apache.heron.proto.tmanager.TopologyManager.MetricsCacheLocation;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MetricsCacheMetricsProviderTest {
  @Test
  public void provides1Comp2InstanceMetricsFromMetricsCache() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    TopologyManager.MetricResponse response = TopologyManager.MetricResponse.newBuilder()
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
        .getMetricsFromMetricsCache(
            metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Collection<Measurement> metrics =
        spyMetricsProvider.getMeasurements(Instant.ofEpochSecond(10),
            Duration.ofSeconds(60),
            metric,
            comp);

    MeasurementsTable table = MeasurementsTable.of(metrics);
    assertEquals(4, table.component(comp).size());
    assertEquals(2, table.uniqueInstances().size());
    assertEquals(1, table.uniqueTypes().size());
    assertEquals(1, table.instance("container_1_bolt_1").size());
    assertEquals(104, table.instance("container_1_bolt_1").sum(), 0.01);
    assertEquals(3, table.instance("container_1_bolt_2").size());
    assertEquals(17, table.instance("container_1_bolt_2").sum(), 0.01);
  }

  @Test
  public void providesMultipleComponentMetricsFromMetricsCache() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp1 = "bolt-1";
    TopologyManager.MetricResponse response1 = TopologyManager.MetricResponse.newBuilder()
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
        .getMetricsFromMetricsCache(
            metric, comp1, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    String comp2 = "bolt-2";
    TopologyManager.MetricResponse response2 = TopologyManager.MetricResponse.newBuilder()
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
        .getMetricsFromMetricsCache(
            metric, comp2, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Collection<Measurement> metrics =
        spyMetricsProvider.getMeasurements(Instant.ofEpochSecond(10),
            Duration.ofSeconds(60),
            Collections.singletonList(metric),
            Arrays.asList(comp1, comp2));

    assertEquals(4, metrics.size());
    MeasurementsTable table = MeasurementsTable.of(metrics);
    assertEquals(2, table.uniqueComponents().size());
    assertEquals(1, table.component(comp1).size());
    assertEquals(104, table.instance("container_1_bolt-1_2").sum(), 0.01);

    assertEquals(3, table.component(comp2).size());
    assertEquals(1, table.uniqueTypes().size());
    assertEquals(3, table.type(metric).instance("container_1_bolt-2_1").size());
    assertEquals(17, table.instance("container_1_bolt-2_1").sum(), 0.01);
  }

  @Test
  public void parsesBackPressureMetric() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "__time_spent_back_pressure_by_compid/container_1_split_1";
    String comp = "__stmgr__";
    TopologyManager.MetricResponse response = TopologyManager.MetricResponse.newBuilder()
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
        .getMetricsFromMetricsCache(
            metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));
    Collection<Measurement> metrics =
        spyMetricsProvider.getMeasurements(Instant.ofEpochSecond(10),
            Duration.ofSeconds(60),
            metric,
            comp);

    assertEquals(1, metrics.size());
    MeasurementsTable table = MeasurementsTable.of(metrics);
    assertEquals(1, table.component(comp).size());
    assertEquals(601, table.instance("stmgr-1").type(metric).sum(), 0.01);
  }

  @Test
  public void handleMissingData() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "dummy";
    String comp = "split";
    TopologyManager.MetricResponse response = TopologyManager.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(
            metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));
    Collection<Measurement> metrics =
        spyMetricsProvider.getMeasurements(Instant.ofEpochSecond(10),
            Duration.ofSeconds(60),
            metric,
            comp);

    assertEquals(0, metrics.size());
  }

  private MetricsCacheMetricsProvider createMetricsProviderSpy() {
    MetricsCacheLocation location = MetricsCacheLocation.newBuilder()
        .setTopologyName("testTopo")
        .setTopologyId("topoId")
        .setHost("localhost")
        .setControllerPort(0)
        .setServerPort(0)
        .build();

    SchedulerStateManagerAdaptor stateMgr = Mockito.mock(SchedulerStateManagerAdaptor.class);
    when(stateMgr.getMetricsCacheLocation("testTopo")).thenReturn(location);

    MetricsCacheMetricsProvider metricsProvider
        = new MetricsCacheMetricsProvider(stateMgr, "testTopo");

    return spy(metricsProvider);
  }

  @Test
  public void testGetTimeLineMetrics() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    TopologyManager.MetricResponse response = TopologyManager.MetricResponse.newBuilder()
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
        .getMetricsFromMetricsCache(
            metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Collection<Measurement> metrics =
        spyMetricsProvider.getMeasurements(Instant.ofEpochSecond(10),
            Duration.ofSeconds(60),
            metric,
            comp);

    assertEquals(4, metrics.size());
    MeasurementsTable table = MeasurementsTable.of(metrics);
    assertEquals(4, table.component(comp).size());

    MeasurementsTable result = table.instance("container_1_bolt_1");
    assertEquals(1, result.size());
    assertEquals(104, result.instant(Instant.ofEpochSecond(1497481288)).sum(), 0.01);

    result = table.instance("container_1_bolt_2");
    assertEquals(3, result.size());
    assertEquals(12, result.instant(Instant.ofEpochSecond(1497481228L)).sum(), 0.01);
    assertEquals(2, result.instant(Instant.ofEpochSecond(1497481348L)).sum(), 0.01);
    assertEquals(3, result.instant(Instant.ofEpochSecond(1497481168L)).sum(), 0.01);
  }
}
