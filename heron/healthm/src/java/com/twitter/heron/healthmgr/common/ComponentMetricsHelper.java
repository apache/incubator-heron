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
import java.util.Map;

import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BUFFER_SIZE;


/**
 * A helper class to compute and hold metrics derived from component metrics
 */
public class ComponentMetricsHelper {
  private final ComponentMetrics componentMetrics;

  private List<InstanceMetrics> boltsWithBackpressure = new ArrayList<>();

  private double bufferChangeRate = 0;
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
      }
    }
  }


  public void computeBufferSizeTrend() {
    for (InstanceMetrics mergedInstance : componentMetrics.getMetrics().values()) {
      Map<Long, Double> bufferMetrics = mergedInstance.getMetrics().get(METRIC_BUFFER_SIZE);
      if (bufferMetrics == null || bufferMetrics.size() < 3) {
        // missing of insufficient data for creating a trend line
        continue;
      }

      SimpleRegression simpleRegression = new SimpleRegression(true);
      for (Long timeStampX : bufferMetrics.keySet()) {
        simpleRegression.addData(timeStampX, bufferMetrics.get(timeStampX));
      }
      bufferChangeRate = simpleRegression.getSlope();
    }
  }


  public MetricsStats computeMinMaxStats(String metric) {
    double metricMax = 0;
    double metricMin = Double.MAX_VALUE;
    for (InstanceMetrics instance : componentMetrics.getMetrics().values()) {

      Double metricValue = instance.getMetricValueSum(metric);
      if (metricValue == null) {
        continue;
      }
      metricMax = metricMax < metricValue ? metricValue : metricMax;
      metricMin = metricMin > metricValue ? metricValue : metricMin;
    }
    return new MetricsStats(metricMin, metricMax);
  }


  public double getTotalBackpressure() {
    return totalBackpressure;
  }

  public double getBufferChangeRate() {
    return bufferChangeRate;
  }

  public List<InstanceMetrics> getBoltsWithBackpressure() {
    return boltsWithBackpressure;
  }
}
