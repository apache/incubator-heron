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

package org.apache.storm.metric.api;

public final class GlobalMetrics {

  private GlobalMetrics() {
  }

  /**
   * Not thread safe increment of counterName. Counter doesn't exist unless incremented once
   */
  public static void incr(String counterName) {
    org.apache.heron.api.metric.GlobalMetrics.incr(counterName);
  }

  /**
   * Not thread safe 'incrementing by' of counterName. Counter doesn't exist unless incremented once
   */
  public static void incrBy(String counterName, int incVal) {
    org.apache.heron.api.metric.GlobalMetrics.incrBy(counterName, incVal);
  }

  /**
   * Thread safe created increment of counterName. (Slow)
   */
  public static void safeIncr(String counterName) {
    org.apache.heron.api.metric.GlobalMetrics.safeIncr(counterName);
  }

  /**
   * Thread safe created increment of counterName. (Slow)
   */
  public static void safeIncrBy(String counterName, int incVal) {
    org.apache.heron.api.metric.GlobalMetrics.safeIncrBy(counterName, incVal);
  }
}
