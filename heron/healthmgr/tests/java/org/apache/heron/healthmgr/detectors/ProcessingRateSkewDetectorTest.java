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

package org.apache.heron.healthmgr.detectors;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;
import com.microsoft.dhalion.core.SymptomsTable;
import com.microsoft.dhalion.policy.PoliciesExecutor;

import org.junit.Test;

import org.apache.heron.healthmgr.HealthPolicyConfig;

import static org.apache.heron.healthmgr.detectors.ProcessingRateSkewDetector.CONF_SKEW_RATIO;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessingRateSkewDetectorTest {

  @Test
  public void testGetMaxMin() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 1.5)).thenReturn(2.5);

    Measurement measurement1 = new Measurement(
        "bolt", "i1", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 1000);
    Measurement measurement2 = new Measurement(
        "bolt", "i1", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892122), 3000);
    Measurement measurement3 = new Measurement(
        "bolt", "i2", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 200.0);
    Measurement measurement4 = new Measurement(
        "bolt", "i2", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 400.0);

    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    metrics.add(measurement3);
    metrics.add(measurement4);

    MeasurementsTable metricsTable = MeasurementsTable.of(metrics);


    ProcessingRateSkewDetector detector = new ProcessingRateSkewDetector(config);

    assertEquals(2000, (int) detector.getMaxOfAverage(metricsTable));
    assertEquals(300, (int) detector.getMinOfAverage(metricsTable));

  }

  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 1.5)).thenReturn(2.5);

    Measurement measurement1 = new Measurement(
        "bolt", "i1", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 1000);
    Measurement measurement2 = new Measurement(
        "bolt", "i2", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 200.0);

    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);

    ProcessingRateSkewDetector detector = new ProcessingRateSkewDetector(config);
    PoliciesExecutor.ExecutionContext context = mock(PoliciesExecutor.ExecutionContext.class);
    when(context.checkpoint()).thenReturn(Instant.now());
    detector.initialize(context);

    Collection<Symptom> symptoms = detector.detect(metrics);

    assertEquals(3, symptoms.size());
    SymptomsTable symptomsTable = SymptomsTable.of(symptoms);
    assertEquals(1, symptomsTable.type("POSITIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).size());
    assertEquals(1, symptomsTable.type("NEGATIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).size());
    assertEquals(1, symptomsTable.type("POSITIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).assignment("i1").size());
    assertEquals(1, symptomsTable.type("NEGATIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).assignment("i2").size());

    measurement1 = new Measurement(
        "bolt", "i1", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 1000);
    measurement2 = new Measurement(
        "bolt", "i2", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 500.0);

    metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);

    detector = new ProcessingRateSkewDetector(config);
    detector.initialize(context);
    symptoms = detector.detect(metrics);

    assertEquals(0, symptoms.size());
  }

  @Test
  public void testReturnsMultipleComponents() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 1.5)).thenReturn(2.5);

    Measurement measurement1 = new Measurement(
        "bolt", "i1", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 1000);
    Measurement measurement2 = new Measurement(
        "bolt", "i2", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 200.0);

    Measurement measurement3 = new Measurement(
        "bolt2", "i3", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 1000);
    Measurement measurement4 = new Measurement(
        "bolt2", "i4", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 200.0);

    Measurement measurement5 = new Measurement(
        "bolt3", "i5", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 1000);
    Measurement measurement6 = new Measurement(
        "bolt3", "i6", METRIC_EXE_COUNT.text(), Instant.ofEpochSecond(1497892222), 500.0);


    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    metrics.add(measurement3);
    metrics.add(measurement4);
    metrics.add(measurement5);
    metrics.add(measurement6);

    ProcessingRateSkewDetector detector = new ProcessingRateSkewDetector(config);
    PoliciesExecutor.ExecutionContext context = mock(PoliciesExecutor.ExecutionContext.class);
    when(context.checkpoint()).thenReturn(Instant.now());
    detector.initialize(context);

    Collection<Symptom> symptoms = detector.detect(metrics);

    assertEquals(6, symptoms.size());

    SymptomsTable symptomsTable = SymptomsTable.of(symptoms);
    assertEquals(2, symptomsTable.type("POSITIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).size());
    assertEquals(2, symptomsTable.type("NEGATIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).size());
    assertEquals(1, symptomsTable.type("POSITIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).assignment("i1").size());
    assertEquals(1, symptomsTable.type("POSITIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).assignment("i3").size());
    assertEquals(1, symptomsTable.type("NEGATIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).assignment("i2").size());
    assertEquals(1, symptomsTable.type("NEGATIVE " + BaseDetector.SymptomType
        .SYMPTOM_PROCESSING_RATE_SKEW).assignment("i4").size());

  }
}
