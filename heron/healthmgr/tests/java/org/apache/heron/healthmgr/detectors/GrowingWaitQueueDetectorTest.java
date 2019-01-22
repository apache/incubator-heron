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
import com.microsoft.dhalion.policy.PoliciesExecutor;

import org.junit.Test;

import org.apache.heron.healthmgr.HealthPolicyConfig;

import static org.apache.heron.healthmgr.detectors.GrowingWaitQueueDetector.CONF_LIMIT;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrowingWaitQueueDetectorTest {
  @Test
  public void testDetector() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_LIMIT, 10.0)).thenReturn(5.0);

    Measurement measurement1 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892222), 0.0);
    Measurement measurement2 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892270), 300.0);
    Measurement measurement3 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892330), 700.0);
    Measurement measurement4 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892390), 1000.0);
    Measurement measurement5 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892450), 1300.0);


    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    metrics.add(measurement3);
    metrics.add(measurement4);
    metrics.add(measurement5);

    GrowingWaitQueueDetector detector = new GrowingWaitQueueDetector(config);
    PoliciesExecutor.ExecutionContext context = mock(PoliciesExecutor.ExecutionContext.class);
    when(context.checkpoint()).thenReturn(Instant.now());
    detector.initialize(context);
    Collection<Symptom> symptoms = detector.detect(metrics);

    assertEquals(1, symptoms.size());
    assertEquals(1, symptoms.iterator().next().assignments().size());

    measurement1 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892222), 0.0);
    measurement2 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892270), 200.0);
    measurement3 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892330), 400.0);
    measurement4 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892390), 600.0);
    measurement5 = new Measurement(
        "bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond(1497892450), 800.0);

    metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    metrics.add(measurement3);
    metrics.add(measurement4);
    metrics.add(measurement5);

    detector = new GrowingWaitQueueDetector(config);
    symptoms = detector.detect(metrics);

    assertEquals(0, symptoms.size());
  }
}
