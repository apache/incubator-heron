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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import com.twitter.heron.healthmgr.sensors.BaseSensor;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_GROWTH_RATE;


/**
 * A helper class to compute and hold metrics derived from component metrics
 */
public class ComponentMetricsHelper {
  private final ComponentMetrics componentMetrics;

  private List<InstanceMetrics> boltsWithBackpressure = new ArrayList<>();
  private double maxBufferChangeRate = 0;
  private double totalBackpressure = 0;

  public ComponentMetricsHelper(ComponentMetrics compMetrics) {
    this.componentMetrics = compMetrics;
  }

  public void computeBpStats() {
    for (InstanceMetrics instanceMetrics : componentMetrics.getMetrics().values()) {
      double bpValue = instanceMetrics.getMetricValueSum(METRIC_BACK_PRESSURE.text());
      if (bpValue > 0) {
        boltsWithBackpressure.add(instanceMetrics);
        totalBackpressure += bpValue;
      }
    }
  }

  public void computeBufferSizeTrend() {
    for (InstanceMetrics instanceMetrics : componentMetrics.getMetrics().values()) {
      Map<Instant, Double> bufferMetrics
          = instanceMetrics.getMetrics().get(METRIC_BUFFER_SIZE.text());
      if (bufferMetrics == null || bufferMetrics.size() < 3) {
        // missing of insufficient data for creating a trend line
        continue;
      }

      SimpleRegression simpleRegression = new SimpleRegression(true);
      for (Instant timestamp : bufferMetrics.keySet()) {
        simpleRegression.addData(timestamp.getEpochSecond(), bufferMetrics.get(timestamp));
      }

      double slope = simpleRegression.getSlope();
      instanceMetrics.addMetric(METRIC_WAIT_Q_GROWTH_RATE.text(), slope);

      if (maxBufferChangeRate < slope) {
        maxBufferChangeRate = slope;
      }
    }
  }

  public MetricsStats computeMinMaxStats(BaseSensor.MetricName metric) {
    return computeMinMaxStats(metric.text());
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

  public double getMaxBufferChangeRate() {
    return maxBufferChangeRate;
  }

  public List<InstanceMetrics> getBoltsWithBackpressure() {
    return boltsWithBackpressure;
  }
}
