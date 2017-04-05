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

package com.twitter.heron.healthmgr.diagnosers;

import com.microsoft.dhalion.api.IDiagnoser;
import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.metrics.InstanceMetricsData;
import com.microsoft.dhalion.symptom.ComponentSymptom;

import com.twitter.heron.healthmgr.common.HealthManagerContstants;

public abstract class BaseDiagnoser implements IDiagnoser<ComponentSymptom> {
  protected static final String EXE_COUNT = HealthManagerContstants.METRIC_EXE_COUNT;
  protected static final String BACK_PRESSURE =
      HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE;

  @Override
  public void close() {
  }

  @Override
  public void initialize() {
  }

  /**
   * A helper class to compute and hold component stats
   */
  protected static class ComponentBackPressureExeStats {
    double sumBPExecuteCounts = 0;
    double sumNonBPExecuteCounts = 0;
    int bpInstanceCount = 0;
    double avgBPExeCount;
    double avgNonBPExeCount;
    int totalInstances;

    public ComponentBackPressureExeStats(ComponentMetricsData backPressureAndExeMetrics) {
      for (InstanceMetricsData mergedInstance : backPressureAndExeMetrics.getMetrics().values()) {
        int bpValue = mergedInstance.getMetricIntValue(BACK_PRESSURE);
        int exeCount = mergedInstance.getMetricIntValue(EXE_COUNT);

        if (bpValue > 0) {
          sumBPExecuteCounts += exeCount;
          bpInstanceCount++;
        } else {
          sumNonBPExecuteCounts += exeCount;
        }
      }

      totalInstances = backPressureAndExeMetrics.getMetrics().size();
      avgBPExeCount = sumBPExecuteCounts / bpInstanceCount;
      avgNonBPExeCount = sumNonBPExecuteCounts / (totalInstances - bpInstanceCount);
    }
  }
}
