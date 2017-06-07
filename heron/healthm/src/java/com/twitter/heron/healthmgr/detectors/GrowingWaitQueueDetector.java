package com.twitter.heron.healthmgr.detectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IDetector;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_GROWING_WAIT_Q;


public class GrowingWaitQueueDetector implements IDetector {
  static final String CONF_LIMIT = GrowingWaitQueueDetector.class.getSimpleName() + ".limit";

  private static final Logger LOG = Logger.getLogger(GrowingWaitQueueDetector.class.getName());
  private final BufferSizeSensor pendingBufferSensor;
  private final double rateLimit;

  @Inject
  GrowingWaitQueueDetector(BufferSizeSensor pendingBufferSensor,
                           HealthPolicyConfig policyConfig) {
    this.pendingBufferSensor = pendingBufferSensor;
    rateLimit = Double.valueOf(policyConfig.getConfig(CONF_LIMIT, "10"));
  }

  /**
   * Detects all components unable to keep up with input load, hence having a growing pending buffer
   * or wait queue
   *
   * @return A collection of all components executing slower than input rate.
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> bufferSizes = pendingBufferSensor.get();
    for (ComponentMetrics compMetrics : bufferSizes.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      compStats.computeBufferSizeTrend();
      if (compStats.getBufferChangeRate() > rateLimit) {
        LOG.info(String.format("Detected growing wait queues for %s, rate %f",
            compMetrics.getName(), compStats.getBufferChangeRate()));
        result.add(new Symptom(SYMPTOM_GROWING_WAIT_Q, compMetrics));
      }
    }

    return result;
  }
}
