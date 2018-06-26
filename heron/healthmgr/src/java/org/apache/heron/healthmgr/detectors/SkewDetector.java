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
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;

import org.apache.heron.healthmgr.sensors.BaseSensor;

public class SkewDetector extends BaseDetector {
  private final double skewRatio;
  private final String metricName;
  private final BaseDetector.SymptomType symptomType;

  @Inject
  SkewDetector(double skewRatio, BaseSensor.MetricName metricName, BaseDetector.SymptomType
      symptomType) {
    this.skewRatio = skewRatio;
    this.metricName = metricName.text();
    this.symptomType = symptomType;
  }

  /**
   * Detects components experiencing skew on a specific metric
   *
   * @return At most two symptoms corresponding to each affected component -- one for positive skew
   * and one for negative skew
   */
  @Override
  public Collection<Symptom> detect(Collection<Measurement> measurements) {
    Collection<Symptom> result = new ArrayList<>();

    MeasurementsTable metrics = MeasurementsTable.of(measurements).type(metricName);
    Instant now = context.checkpoint();
    for (String component : metrics.uniqueComponents()) {
      Set<String> addresses = new HashSet<>();
      Set<String> positiveAddresses = new HashSet<>();
      Set<String> negativeAddresses = new HashSet<>();

      double componentMax = getMaxOfAverage(metrics.component(component));
      double componentMin = getMinOfAverage(metrics.component(component));
      if (componentMax > skewRatio * componentMin) {
        //there is skew
        addresses.add(component);
        result.add(new Symptom(symptomType.text(), now, addresses));

        for (String instance : metrics.component(component).uniqueInstances()) {
          if (metrics.instance(instance).mean() >= 0.90 * componentMax) {
            positiveAddresses.add(instance);
          }
          if (metrics.instance(instance).mean() <= 1.10 * componentMin) {
            negativeAddresses.add(instance);
          }
        }

        if (!positiveAddresses.isEmpty()) {
          result.add(new Symptom("POSITIVE " + symptomType.text(), now, positiveAddresses));
        }
        if (!negativeAddresses.isEmpty()) {
          result.add(new Symptom("NEGATIVE " + symptomType.text(), now, negativeAddresses));
        }
      }

    }
    return result;
  }

  @VisibleForTesting
  double getMaxOfAverage(MeasurementsTable table) {
    double max = 0;
    for (String instance : table.uniqueInstances()) {
      double instanceMean = table.instance(instance).mean();
      if (instanceMean > max) {
        max = instanceMean;
      }
    }
    return max;
  }

  @VisibleForTesting
  double getMinOfAverage(MeasurementsTable table) {
    double min = Double.MAX_VALUE;
    for (String instance : table.uniqueInstances()) {
      double instanceMean = table.instance(instance).mean();
      if (instanceMean < min) {
        min = instanceMean;
      }
    }
    return min;
  }
}
