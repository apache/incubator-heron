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


package org.apache.heron.streamlet.impl;


import java.time.Duration;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.windowing.EvictionPolicy;
import org.apache.heron.api.windowing.TriggerPolicy;
import org.apache.heron.streamlet.WindowConfig;

/**
 * WindowConfigImpl implements the WindowConfig interface.
 */
public final class WindowConfigImpl implements WindowConfig {
  private enum WindowType { TIME, COUNT, CUSTOM }
  private WindowType windowType;
  private int windowSize;
  private int slideInterval;
  private Duration windowDuration;
  private Duration slidingIntervalDuration;
  private TriggerPolicy<Tuple, ?> triggerPolicy;
  private EvictionPolicy<Tuple, ?> evictionPolicy;

  public  WindowConfigImpl(Duration windowDuration, Duration slidingIntervalDuration) {
    this.windowType = WindowType.TIME;
    this.windowDuration = windowDuration;
    this.slidingIntervalDuration = slidingIntervalDuration;
  }
  public WindowConfigImpl(int windowSize, int slideInterval) {
    this.windowType = WindowType.COUNT;
    this.windowSize = windowSize;
    this.slideInterval = slideInterval;
  }
  public WindowConfigImpl(TriggerPolicy<Tuple, ?> triggerPolicy,
                          EvictionPolicy<Tuple, ?> evictionPolicy) {
    this.windowType = WindowType.CUSTOM;
    this.triggerPolicy = triggerPolicy;
    this.evictionPolicy = evictionPolicy;
  }

  public void attachWindowConfig(BaseWindowedBolt bolt) {
    switch(windowType) {
      case COUNT:
        bolt.withWindow(BaseWindowedBolt.Count.of(windowSize),
                        BaseWindowedBolt.Count.of(slideInterval));
        break;
      case TIME:
        bolt.withWindow(windowDuration, slidingIntervalDuration);
        break;
      case CUSTOM:
        bolt.withCustomEvictor(evictionPolicy);
        bolt.withCustomTrigger(triggerPolicy);
        break;
      default:
        throw new RuntimeException("Unknown windowType " + String.valueOf(windowType));
    }
  }
}
