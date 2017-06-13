package com.twitter.heron.healthmgr.common;

import java.util.ArrayList;
import java.util.List;

import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.*;

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
      double bpValue = instanceMetrics.getMetricValue(METRIC_BACK_PRESSURE);
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
      Double bufferSize = mergedInstance.getMetricValue(METRIC_BUFFER_SIZE);
      if (bufferSize == null) {
        continue;
      }
      bufferSizeMax = bufferSizeMax < bufferSize ? bufferSize : bufferSizeMax;
      bufferSizeMin = bufferSizeMin > bufferSize ? bufferSize : bufferSizeMin;
    }
  }

  public void computeExeCountStats() {
    for (InstanceMetrics instance : componentMetrics.getMetrics().values()) {
      double exeCount = instance.getMetricValue(METRIC_EXE_COUNT);
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
