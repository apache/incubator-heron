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

package org.apache.heron.api.metric;

import java.io.Serializable;
import java.time.Duration;

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
  public static final String ROOT_NAME = "__auto__";
  private MultiCountMetric metricsContainer;
  private boolean registered;

  GlobalMetrics() {
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
  public static void incrBy(String counterName, int incrValue) {
    INSTANCE.metricsContainer.scope(counterName).incrBy(incrValue);
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
   * Thread safe created increment of counterName. (Slow)
   */
  public static void safeIncrBy(String counterName, int incrValue) {
    synchronized (INSTANCE) {
      if (INSTANCE.registered) {
        INSTANCE.metricsContainer.scope(counterName).incrBy(incrValue);
      }
    }
  }

  /**
   * Initialize the counter by registering the metricContainer. Should be done in open/prepare call.
   * TODO: Investigate if it is possible to do this part in ctor. One thing to note is how this will
   * affect the serialization of CounterFactory.
   */
  public static void init(IMetricsRegister metricsRegister, Duration metricsBucket) {
    synchronized (INSTANCE) {
      if (!INSTANCE.registered) {
        metricsRegister.registerMetric(
            ROOT_NAME, INSTANCE.metricsContainer, (int) metricsBucket.getSeconds());
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

  /**
   * During serialization don't create a copy of this class. 'readResolve' is used by reflection
   * for java serialization.
   */
  protected Object readResolve() {
    return INSTANCE;
  }
}
