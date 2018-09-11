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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.Symptom;
import com.microsoft.dhalion.core.SymptomsTable;
import com.microsoft.dhalion.policy.PoliciesExecutor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.healthmgr.HealthManagerMetrics;
import org.apache.heron.healthmgr.HealthPolicyConfig;

import static org.apache.heron.healthmgr.detectors.BackPressureDetector.CONF_NOISE_FILTER;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_COMP_BACK_PRESSURE;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_INSTANCE_BACK_PRESSURE;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackPressureDetectorTest {
  private Instant now;

  @Before
  public void setup() {
    now = Instant.now();
  }
  @Test
  public void testConfigAndFilter() throws IOException {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_NOISE_FILTER, 20)).thenReturn(50);


    Measurement measurement1
        = new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), now, 55);
    Measurement measurement2
        = new Measurement("bolt", "i2", METRIC_BACK_PRESSURE.text(), now, 3);
    Measurement measurement3
        = new Measurement("bolt", "i3", METRIC_BACK_PRESSURE.text(), now, 0);
    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    metrics.add(measurement3);

    HealthManagerMetrics publishingMetrics = mock(HealthManagerMetrics.class);
    BackPressureDetector detector = new BackPressureDetector(config, publishingMetrics);
    PoliciesExecutor.ExecutionContext context = mock(PoliciesExecutor.ExecutionContext.class);
    when(context.checkpoint()).thenReturn(now);
    detector.initialize(context);
    Collection<Symptom> symptoms = detector.detect(metrics);

    Assert.assertEquals(2, symptoms.size());
    SymptomsTable compSymptom = SymptomsTable.of(symptoms).type(SYMPTOM_COMP_BACK_PRESSURE.text());
    Assert.assertEquals(1, compSymptom.size());
    Assert.assertEquals(1, compSymptom.get().iterator().next().assignments().size());

    SymptomsTable instanceSymptom
        = SymptomsTable.of(symptoms).type(SYMPTOM_INSTANCE_BACK_PRESSURE.text());
    Assert.assertEquals(1, instanceSymptom.size());
    Assert.assertEquals(1, instanceSymptom.get().iterator().next().assignments().size());

    Symptom symptom = symptoms.iterator().next();


    measurement1
        = new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), now, 45);
    measurement2
        = new Measurement("bolt", "i2", METRIC_BACK_PRESSURE.text(), now, 3);
    metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);

    detector = new BackPressureDetector(config, publishingMetrics);
    detector.initialize(context);
    symptoms = detector.detect(metrics);

    Assert.assertEquals(0, symptoms.size());
  }
}
