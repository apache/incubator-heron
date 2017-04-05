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


import java.util.HashMap;
import java.util.Map;

import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.metrics.InstanceMetricsData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class TrackerMetricsProviderTest {
  @Test
  public void providesOneComponentMetricsFromTracker() {
    TrackerMetricsProvider metricsProvider
        = new TrackerMetricsProvider("localhost", "dev", "env", "topology");

    TrackerMetricsProvider spyMetricsProvider = spy(metricsProvider);

    String metric = "count";
    String component = "bolt";
    String response = "{\"status\": \"\", " + "\"executiontime\": 0.0026040077209472656, " +
        "\"message\": \"\", \"version\": \"\", " +
        "\"result\": {\"metrics\": {\"count\": {\"container_1_bolt_2\": \"496\"" +
        ", \"container_1_bolt_1\": \"104\"}}, " +
        "\"interval\": 60, \"component\": \"bolt\"}}";

    doReturn(response).when(spyMetricsProvider).getMetricsFromTracker(metric, component, 60);
    Map<String, ComponentMetricsData> metrics
        = spyMetricsProvider.getComponentMetrics(metric, 60, component);

    assertEquals(1, metrics.size());
    assertNotNull(metrics.get(component));
    assertEquals(2, metrics.get(component).getMetrics().size());

    HashMap<String, InstanceMetricsData> componentMetrics = metrics.get(component).getMetrics();
    assertEquals(104, componentMetrics.get("container_1_bolt_1").getMetricIntValue("count"));
    assertEquals(496, componentMetrics.get("container_1_bolt_2").getMetricIntValue("count"));
  }

  @Test
  public void providesMultipleComponentMetricsFromTracker() {
    TrackerMetricsProvider metricsProvider
        = new TrackerMetricsProvider("localhost", "dev", "env", "topology");

    TrackerMetricsProvider spyMetricsProvider = spy(metricsProvider);

    String metric = "count";
    String comp1 = "bolt-1";
    String response1 = "{\"status\": \"\", " + "\"executiontime\": 0.0026040077209472656, " +
        "\"message\": \"\", \"version\": \"\", " +
        "\"result\": {\"metrics\": {\"count\": {\"container_1_bolt-1_2\": \"496\"}}, " +
        "\"interval\": 60, \"component\": \"bolt-1\"}}";
    doReturn(response1).when(spyMetricsProvider).getMetricsFromTracker(metric, comp1, 60);

    String comp2 = "bolt-2";
    String response2 = "{\"status\": \"\", " + "\"executiontime\": 0.0026040077209472656, " +
        "\"message\": \"\", \"version\": \"\", " +
        "\"result\": {\"metrics\": {\"count\": {\"container_1_bolt-2_1\": \"123\"}}, " +
        "\"interval\": 60, \"component\": \"bolt-2\"}}";
    doReturn(response2).when(spyMetricsProvider).getMetricsFromTracker(metric, comp2, 60);

    Map<String, ComponentMetricsData> metrics
        = spyMetricsProvider.getComponentMetrics(metric, 60, comp1, comp2);

    assertEquals(2, metrics.size());
    assertNotNull(metrics.get(comp1));
    assertEquals(1, metrics.get(comp1).getMetrics().size());
    assertEquals(496,
        metrics.get(comp1).getMetricValue("container_1_bolt-1_2", "count").intValue());

    assertNotNull(metrics.get(comp2));
    assertEquals(1, metrics.get(comp2).getMetrics().size());
    assertEquals(123,
        metrics.get(comp2).getMetricValue("container_1_bolt-2_1", "count").intValue());
  }

  @Test
  public void parsesBackPressureMetric() {
    TrackerMetricsProvider metricsProvider
        = new TrackerMetricsProvider("localhost", "dev", "env", "topology");

    TrackerMetricsProvider spyMetricsProvider = spy(metricsProvider);

    String metric = "__time_spent_back_pressure_by_compid/container_1_split_1";
    String component = "__stmgr__";
    String response = "{\"status\": \"success\", " +
        "\"executiontime\": 0.30, \"message\": \"\", \"version\": \"v\", " +
        "\"result\": " +
        "{\"metrics\": {\"__time_spent_back_pressure_by_compid/container_1_split_1\": " +
        "{\"stmgr-1\": \"601\"}}, " +
        "\"interval\": 60, \"component\": \"__stmgr__\"}}";

    doReturn(response).when(spyMetricsProvider).getMetricsFromTracker(metric, component, 60);
    Map<String, ComponentMetricsData> metrics
        = spyMetricsProvider.getComponentMetrics(metric, 60, component);

    assertEquals(1, metrics.size());
    assertNotNull(metrics.get(component));
    assertEquals(1, metrics.get(component).getMetrics().size());

    HashMap<String, InstanceMetricsData> componentMetrics = metrics.get(component).getMetrics();
    assertEquals(601, componentMetrics.get("stmgr-1").getMetricIntValue(metric));
  }

  @Test
  public void handleMissingData() {
    TrackerMetricsProvider metricsProvider
        = new TrackerMetricsProvider("localhost", "dev", "env", "topology");

    TrackerMetricsProvider spyMetricsProvider = spy(metricsProvider);

    String metric = "dummy";
    String component = "split";
    String response = "{\"status\": \"success\", \"executiontime\": 0.30780792236328125, " +
        "\"message\": \"\", \"version\": \"v\", \"result\": " +
        "{\"metrics\": {}, \"interval\": 0, \"component\": \"split\"}}";

    doReturn(response).when(spyMetricsProvider).getMetricsFromTracker(metric, component, 60);
    Map<String, ComponentMetricsData> metrics
        = spyMetricsProvider.getComponentMetrics(metric, 60, component);

    assertEquals(1, metrics.size());
    assertNotNull(metrics.get(component));
    assertEquals(0, metrics.get(component).getMetrics().size());
  }
}
