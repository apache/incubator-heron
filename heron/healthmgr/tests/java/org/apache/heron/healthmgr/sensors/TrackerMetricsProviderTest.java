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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class TrackerMetricsProviderTest {
  @Test
  public void provides1Comp2InstanceMetricsFromTracker() {
    TrackerMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    String response = "{\"status\": \"success\", \"executiontime\": 0.002241849899291992, "
        + "\"message\": \"\", \"version\": \"ver\", \"result\": "
        + "{\"timeline\": {\"count\": "
        + "{\"container_1_bolt_1\": {\"1497481288\": \"104\"}, "
        + "\"container_1_bolt_2\": {\"1497481228\": \"12\", \"1497481348\": \"2\", "
        + "\"1497481168\": \"3\"}}}, "
        + "\"endtime\": 1497481388, \"component\": \"bolt\", \"starttime\": 1497481208}}";

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromTracker(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Collection<Measurement> metrics =
        spyMetricsProvider.getMeasurements(Instant.ofEpochSecond(10),
            Duration.ofSeconds(60),
            metric,
            comp);

    assertEquals(4, metrics.size());
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
  public void providesMultipleComponentMetricsFromTracker() {
    TrackerMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp1 = "bolt-1";
    String response1 = "{\"status\": \"success\", \"executiontime\": 0.002241849899291992, "
        + "\"message\": \"\", \"version\": \"ver\", \"result\": "
        + "{\"timeline\": {\"count\": "
        + "{\"container_1_bolt-1_2\": {\"1497481288\": \"104\"}"
        + "}}, "
        + "\"endtime\": 1497481388, \"component\": \"bolt\", \"starttime\": 1497481208}}";

    doReturn(response1).when(spyMetricsProvider)
        .getMetricsFromTracker(metric, comp1, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    String comp2 = "bolt-2";
    String response2 = "{\"status\": \"\", " + "\"executiontime\": 0.0026040077209472656, "
        + "\"message\": \"\", \"version\": \"\", "
        + "\"result\": {\"timeline\": {\"count\": "
        + "{\"container_1_bolt-2_1\": {\"1497481228\": \"12\", \"1497481348\": \"2\", "
        + "\"1497481168\": \"3\"}}}, "
        + "\"interval\": 60, \"component\": \"bolt-2\"}}";
    doReturn(response2).when(spyMetricsProvider)
        .getMetricsFromTracker(metric, comp2, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

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
    TrackerMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "__time_spent_back_pressure_by_compid/container_1_split_1";
    String comp = "__stmgr__";
    String response = "{\"status\": \"success\", "
        + "\"executiontime\": 0.30, \"message\": \"\", \"version\": \"v\", "
        + "\"result\": "
        + "{\"metrics\": {\"__time_spent_back_pressure_by_compid/container_1_split_1\": "
        + "{\"stmgr-1\": {\"00\" : \"601\"}}}, "
        + "\"interval\": 60, \"component\": \"__stmgr__\"}}";

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromTracker(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

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
    TrackerMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "dummy";
    String comp = "split";
    String response = "{\"status\": \"success\", \"executiontime\": 0.30780792236328125, "
        + "\"message\": \"\", \"version\": \"v\", \"result\": "
        + "{\"metrics\": {}, \"interval\": 0, \"component\": \"split\"}}";

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromTracker(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    Collection<Measurement> metrics =
        spyMetricsProvider.getMeasurements(Instant.ofEpochSecond(10),
            Duration.ofSeconds(60),
            metric,
            comp);

    assertEquals(0, metrics.size());
  }

  private TrackerMetricsProvider createMetricsProviderSpy() {
    TrackerMetricsProvider metricsProvider
        = new TrackerMetricsProvider("127.0.0.1", "topology", "dev", "env");

    TrackerMetricsProvider spyMetricsProvider = spy(metricsProvider);
    return spyMetricsProvider;
  }

  @Test
  public void testGetTimeLineMetrics() {
    TrackerMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    String response = "{\"status\": \"success\", \"executiontime\": 0.002241849899291992, "
        + "\"message\": \"\", \"version\": \"ver\", \"result\": "
        + "{\"timeline\": {\"count\": "
        + "{\"container_1_bolt_1\": {\"1497481288\": \"104\"}, "
        + "\"container_1_bolt_2\": {\"1497481228\": \"12\", \"1497481348\": \"2\", "
        + "\"1497481168\": \"3\"}}}, "
        + "\"endtime\": 1497481388, \"component\": \"bolt\", \"starttime\": 1497481208}}";

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromTracker(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

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
