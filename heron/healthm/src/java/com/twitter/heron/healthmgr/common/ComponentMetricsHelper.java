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

package com.twitter.heron.healthmgr.common;

import java.util.ArrayList;
import java.util.List;

import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_EXE_COUNT;


/**
 * A helper class to compute and hold metrics derived from component metrics
 */
public class ComponentMetricsHelper {
  private final ComponentMetrics componentMetrics;

  private List<InstanceMetrics> boltsWithBackpressure = new ArrayList<>();
  private List<InstanceMetrics> boltsWithoutBackpressure = new ArrayList<>();
  private double exeCountMax = 0;
  private double exeCountMin = Double.MAX_VALUE;
  private double bufferSizeMax = 0;
  private double bufferSizeMin = Double.MAX_VALUE;
  private double totalBackpressure = 0;

  public ComponentMetricsHelper(ComponentMetrics compMetrics) {
    this.componentMetrics = compMetrics;
  }

  public void computeBpStats() {
    for (InstanceMetrics instanceMetrics : componentMetrics.getMetrics().values()) {
      double bpValue = instanceMetrics.getMetricValueSum(METRIC_BACK_PRESSURE);
      if (bpValue > 0) {
        boltsWithBackpressure.add(instanceMetrics);
        totalBackpressure += bpValue;
      } else {
        boltsWithoutBackpressure.add(instanceMetrics);
      }
    }
  }

  public void computeBufferSizeStats() {
    for (InstanceMetrics mergedInstance : componentMetrics.getMetrics().values()) {
      Double bufferSize = mergedInstance.getMetricValueSum(METRIC_BUFFER_SIZE);
      if (bufferSize == null) {
        continue;
      }
      bufferSizeMax = bufferSizeMax < bufferSize ? bufferSize : bufferSizeMax;
      bufferSizeMin = bufferSizeMin > bufferSize ? bufferSize : bufferSizeMin;
    }
  }

  public void computeExeCountStats() {
    for (InstanceMetrics instance : componentMetrics.getMetrics().values()) {
      double exeCount = instance.getMetricValueSum(METRIC_EXE_COUNT);
      exeCountMax = exeCountMax < exeCount ? exeCount : exeCountMax;
      exeCountMin = exeCountMin > exeCount ? exeCount : exeCountMin;
    }
  }

  public double getExeCountMax() {
    return exeCountMax;
  }

  public double getExeCountMin() {
    return exeCountMin;
  }

  public double getBufferSizeMax() {
    return bufferSizeMax;
  }

  public double getBufferSizeMin() {
    return bufferSizeMin;
  }

  public double getTotalBackpressure() {
    return totalBackpressure;
  }

  public List<InstanceMetrics> getBoltsWithBackpressure() {
    return boltsWithBackpressure;
  }

  public List<InstanceMetrics> getBoltsWithoutBackpressure() {
    return boltsWithoutBackpressure;
  }
}
