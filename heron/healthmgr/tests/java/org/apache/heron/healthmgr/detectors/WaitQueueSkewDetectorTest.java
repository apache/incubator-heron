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
import com.microsoft.dhalion.core.Symptom;
import com.microsoft.dhalion.core.SymptomsTable;
import com.microsoft.dhalion.policy.PoliciesExecutor;

import org.junit.Test;

import org.apache.heron.healthmgr.HealthPolicyConfig;

import static org.apache.heron.healthmgr.detectors.WaitQueueSkewDetector.CONF_SKEW_RATIO;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WaitQueueSkewDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 20.0)).thenReturn(15.0);

    Measurement measurement1 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892222), 1501);
    Measurement measurement2 = new Measurement(
        "bolt", "i2", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892222), 100.0);

    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);

    WaitQueueSkewDetector detector = new WaitQueueSkewDetector(config);
    PoliciesExecutor.ExecutionContext context = mock(PoliciesExecutor.ExecutionContext.class);
    when(context.checkpoint()).thenReturn(Instant.now());
    detector.initialize(context);
    Collection<Symptom> symptoms = detector.detect(metrics);

    assertEquals(3, symptoms.size());
    SymptomsTable symptomsTable = SymptomsTable.of(symptoms);
    assertEquals(1, symptomsTable.type("POSITIVE " + BaseDetector.SymptomType
        .SYMPTOM_WAIT_Q_SIZE_SKEW).size());
    assertEquals(1, symptomsTable.type("NEGATIVE " + BaseDetector.SymptomType
        .SYMPTOM_WAIT_Q_SIZE_SKEW).size());
    assertEquals(1, symptomsTable.type("POSITIVE " + BaseDetector.SymptomType
        .SYMPTOM_WAIT_Q_SIZE_SKEW).assignment("i1").size());
    assertEquals(1, symptomsTable.type("NEGATIVE " + BaseDetector.SymptomType
        .SYMPTOM_WAIT_Q_SIZE_SKEW).assignment("i2").size());

    measurement1 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892222), 1500);
    measurement2 = new Measurement(
        "bolt", "i2", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892222), 110.0);

    metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);

    detector = new WaitQueueSkewDetector(config);
    detector.initialize(context);
    symptoms = detector.detect(metrics);

    assertEquals(0, symptoms.size());
  }
}
