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
import java.util.Collection;
import java.util.logging.Logger;

import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;
import com.microsoft.dhalion.core.SymptomsTable;

import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_COMP_BACK_PRESSURE;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_PROCESSING_RATE_SKEW;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_WAIT_Q_SIZE_SKEW;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_DATA_SKEW;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;

public class DataSkewDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(DataSkewDiagnoser.class.getName());

  @Override
  public Collection<Diagnosis> diagnose(Collection<Symptom> symptoms) {
    Collection<Diagnosis> diagnoses = new ArrayList<>();
    SymptomsTable symptomsTable = SymptomsTable.of(symptoms);

    SymptomsTable bp = symptomsTable.type(SYMPTOM_COMP_BACK_PRESSURE.text());
    if (bp.size() > 1) {
      // TODO handle cases where multiple detectors create back pressure symptom
      throw new IllegalStateException("Multiple back-pressure symptoms case");
    }
    if (bp.size() == 0) {
      return diagnoses;
    }
    String bpComponent = bp.first().assignments().iterator().next();

    SymptomsTable processingRateSkew = symptomsTable.type(SYMPTOM_PROCESSING_RATE_SKEW.text());
    SymptomsTable waitQSkew = symptomsTable.type(SYMPTOM_WAIT_Q_SIZE_SKEW.text());

    // verify data skew, larger queue size and back pressure for the same component exists
    if (waitQSkew.assignment(bpComponent).size() == 0
        || processingRateSkew.assignment(bpComponent).size() == 0) {
      return diagnoses;
    }

    Collection<String> assignments = new ArrayList<>();

    Instant newest = context.checkpoint();
    Instant oldest = context.previousCheckpoint();
    MeasurementsTable measurements = context.measurements()
        .between(oldest, newest)
        .component(bpComponent);

    for (String instance : measurements.uniqueInstances()) {
      MeasurementsTable instanceMeasurements = measurements.instance(instance);
      double waitQSize = instanceMeasurements.type(METRIC_WAIT_Q_SIZE.text()).mean();
      double processingRate = instanceMeasurements.type(METRIC_EXE_COUNT.text()).mean();
      if ((measurements.type(METRIC_WAIT_Q_SIZE.text()).max() < waitQSize * 2)
          && (measurements.type(METRIC_EXE_COUNT.text()).max() < 1.10 * processingRate)) {
        assignments.add(instance);
        LOG.info(String.format("DataSkew: %s back-pressure, high execution count: %s and "
            + "high buffer size %s", instance, processingRate, waitQSize));
      }
    }

    if (assignments.size() > 0) {
      diagnoses.add(new Diagnosis(DIAGNOSIS_DATA_SKEW.text(), context.checkpoint(), assignments));
    }

    return diagnoses;
  }
}
