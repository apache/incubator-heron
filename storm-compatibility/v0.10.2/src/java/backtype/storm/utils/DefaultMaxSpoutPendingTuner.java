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

package backtype.storm.utils;

/**
 * This is a class that helps to auto tune the max spout pending value
 */
public class DefaultMaxSpoutPendingTuner {
  private org.apache.heron.api.utils.DefaultMaxSpoutPendingTuner delegate;

  /**
   * Conv constructor when initing from a non-set initial value
   */
  public DefaultMaxSpoutPendingTuner(float autoTuneFactor, double progressBound) {
    this(null, autoTuneFactor, progressBound);
  }

  public DefaultMaxSpoutPendingTuner(Long maxSpoutPending, float autoTuneFactor,
                                     double progressBound) {
    delegate = new org.apache.heron.api.utils.DefaultMaxSpoutPendingTuner(maxSpoutPending,
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
