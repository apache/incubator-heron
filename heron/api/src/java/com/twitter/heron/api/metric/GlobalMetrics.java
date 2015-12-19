package com.twitter.heron.api.metric;

import java.io.Serializable;

/**
 * Singleton class which exposes a simple globally available counter for heron jobs.
 * Anywhere in the execution of heron job, user can put-
 * <code>GlobalMetrics.incr("mycounter")</code> to add a counter. There will be no need to
 * explicitly declare the counter before. If the counter doesn't exist it will be created.
 * The creation is lazy which means, unless the counter not being available, it is counted as 0
 * CounterFactory.init() should be called in prepare and open methods of bolt and spout respectively.
 * The counters will be named __auto__/mycounter (note the __auto__ prefix)
 */
public enum GlobalMetrics implements Serializable {
  INSTANCE;
  public static String ROOT_NAME = "__auto__";
  private MultiCountMetric metricsContainer;
  private boolean registered;

  private GlobalMetrics() {
    metricsContainer = new MultiCountMetric();
    registered = false;
  }

  /**
   * Not thread safe increment of counterName. Counter doesn't exist unless incremented once
   */
  public static void incr(String counterName) {
    INSTANCE.metricsContainer.scope(counterName).incr();
  }

  /**
   * Not thread safe 'incrementing by' of counterName. Counter doesn't exist unless incremented once
   */
  public static void incrBy(String counterName, int N) {
    INSTANCE.metricsContainer.scope(counterName).incrBy(N);
  }

  /**
   * Thread safe created increment of counterName. (Slow)
   */
  public static void safeIncr(String counterName) {
    synchronized (INSTANCE) {
      if (INSTANCE.registered) {
        INSTANCE.metricsContainer.scope(counterName).incr();
      }
    }
  }

  /**
   * During serialization don't create a copy of this class. 'readResolve' is used by reflection
   * for java serialization.
   */
  protected Object readResolve() {
    return INSTANCE;
  }

  /**
   * Thread safe created increment of counterName. (Slow)
   */
  public static void safeIncrBy(String counterName, int N) {
    synchronized (INSTANCE) {
      if (INSTANCE.registered) {
        INSTANCE.metricsContainer.scope(counterName);
      }
    }
  }

  /**
   * Initialize the counter by registering the metricContainer. Should be done in open/prepare call.
   * TODO: Investigate if it is possible to do this part in ctor. One thing to note is how this will
   * affect the serialization of CounterFactory.
   */
  public static void init(IMetricsRegister metricsRegister, int metricsBucket) {
    synchronized (INSTANCE) {
      if (!INSTANCE.registered) {
        metricsRegister.registerMetric(ROOT_NAME, INSTANCE.metricsContainer, metricsBucket);
        INSTANCE.registered = true;
      }
    }
  }

  /**
   * test-only
   */
  public static MultiCountMetric getUnderlyingCounter() {
    return INSTANCE.metricsContainer;
  }
}
