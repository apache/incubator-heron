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

package org.apache.heron.healthmgr.diagnosers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.microsoft.dhalion.api.IDiagnoser;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

import org.junit.Before;
import org.junit.Test;

import org.apache.heron.healthmgr.sensors.BaseSensor.MetricName;

import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_COMP_BACK_PRESSURE;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_PROCESSING_RATE_SKEW;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_WAIT_Q_SIZE_SKEW;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_DATA_SKEW;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataSkewDiagnoserTest {
  private final String comp = "comp";
  private Instant now = Instant.now();
  private Collection<Measurement> measurements = new ArrayList<>();
  private ExecutionContext context;
  private IDiagnoser diagnoser;

  @Before
  public void initTestData() {
    now = Instant.now();
    measurements = new ArrayList<>();

    context = mock(ExecutionContext.class);
    when(context.checkpoint()).thenReturn(now);

    diagnoser = new DataSkewDiagnoser();
    diagnoser.initialize(context);
  }

  @Test
  public void failsIfNoDataSkewSymptom() {
    Symptom symptom = new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), Instant.now(), null);
    Collection<Symptom> symptoms = Collections.singletonList(symptom);
    Collection<Diagnosis> result = diagnoser.diagnose(symptoms);
    assertEquals(0, result.size());
  }

  @Test
  public void diagnosis1DataSkewInstance() {
    addMeasurements(METRIC_BACK_PRESSURE, 123, 0, 0);
    addMeasurements(METRIC_EXE_COUNT, 5000, 2000, 2000);
    addMeasurements(METRIC_WAIT_Q_SIZE, 10000, 500, 500);
    when(context.measurements()).thenReturn(MeasurementsTable.of(measurements));

    Collection<String> assign = Collections.singleton(comp);
    Symptom bpSymptom = new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), now, assign);
    Symptom skewSymptom = new Symptom(SYMPTOM_PROCESSING_RATE_SKEW.text(), now, assign);
    Symptom qDisparitySymptom = new Symptom(SYMPTOM_WAIT_Q_SIZE_SKEW.text(), now, assign);
    Collection<Symptom> symptoms = Arrays.asList(bpSymptom, skewSymptom, qDisparitySymptom);

    Collection<Diagnosis> result = diagnoser.diagnose(symptoms);
    assertEquals(1, result.size());
    Diagnosis diagnoses = result.iterator().next();
    assertEquals(DIAGNOSIS_DATA_SKEW.text(), diagnoses.type());
    assertEquals(1, diagnoses.assignments().size());
    assertEquals("i1", diagnoses.assignments().iterator().next());
    // TODO
//    assertEquals(1, diagnoses.symptoms().size());
  }

  @Test
  public void diagnosisNoDataSkewLowBufferSize() {
    addMeasurements(METRIC_BACK_PRESSURE, 123, 0, 0);
    addMeasurements(METRIC_EXE_COUNT, 5000, 2000, 2000);
    addMeasurements(METRIC_WAIT_Q_SIZE, 1, 500, 500);
    when(context.measurements()).thenReturn(MeasurementsTable.of(measurements));

    Collection<String> assign = Collections.singleton(comp);
    Symptom bpSymptom = new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), now, assign);
    Symptom skewSymptom = new Symptom(SYMPTOM_PROCESSING_RATE_SKEW.text(), now, assign);
    Symptom qDisparitySymptom = new Symptom(SYMPTOM_WAIT_Q_SIZE_SKEW.text(), now, assign);

    Collection<Symptom> symptoms = Arrays.asList(bpSymptom, skewSymptom, qDisparitySymptom);
    Collection<Diagnosis> result = diagnoser.diagnose(symptoms);
    assertEquals(0, result.size());
  }

  @Test
  public void diagnosisNoDataSkewLowRate() {
    addMeasurements(METRIC_BACK_PRESSURE, 123, 0, 0);
    addMeasurements(METRIC_EXE_COUNT, 100, 2000, 2000);
    addMeasurements(METRIC_WAIT_Q_SIZE, 10000, 500, 500);
    when(context.measurements()).thenReturn(MeasurementsTable.of(measurements));

    Collection<String> assign = Collections.singleton(comp);
    Symptom bpSymptom = new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), now, assign);
    Symptom skewSymptom = new Symptom(SYMPTOM_PROCESSING_RATE_SKEW.text(), now, assign);
    Symptom qDisparitySymptom = new Symptom(SYMPTOM_WAIT_Q_SIZE_SKEW.text(), now, assign);

    Collection<Symptom> symptoms = Arrays.asList(bpSymptom, skewSymptom, qDisparitySymptom);
    Collection<Diagnosis> result = diagnoser.diagnose(symptoms);
    assertEquals(0, result.size());
  }

  private void addMeasurements(MetricName metricExeCount, int i1, int i2, int i3) {
    measurements.add(new Measurement(comp, "i1", metricExeCount.text(), now, i1));
    measurements.add(new Measurement(comp, "i2", metricExeCount.text(), now, i2));
    measurements.add(new Measurement(comp, "i3", metricExeCount.text(), now, i3));
  }
}
