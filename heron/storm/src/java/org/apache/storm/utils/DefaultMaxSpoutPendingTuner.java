package org.apache.storm.utils;

/**
 * This is a class that helps to auto tune the max spout pending value
 */
public class DefaultMaxSpoutPendingTuner {
  com.twitter.heron.api.utils.DefaultMaxSpoutPendingTuner delegate;

  /** Conv constructor when initing from a non-set initial value */
  public DefaultMaxSpoutPendingTuner(float autoTuneFactor, double progressBound) {
    this(null, autoTuneFactor, progressBound);
  }

  public DefaultMaxSpoutPendingTuner(Long maxSpoutPending, float autoTuneFactor,
                                     double progressBound) {
    delegate = new com.twitter.heron.api.utils.DefaultMaxSpoutPendingTuner(maxSpoutPending,
                                                                           autoTuneFactor,
                                                                           progressBound);
  }

  public Long get() {
    return delegate.get();
  }

  public void autoTune(Long progress) {
    delegate.autoTune(progress);
  }
}
