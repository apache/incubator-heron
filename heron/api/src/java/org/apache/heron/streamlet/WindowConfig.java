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


package org.apache.heron.streamlet;


import java.time.Duration;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.windowing.EvictionPolicy;
import org.apache.heron.api.windowing.TriggerPolicy;
import org.apache.heron.streamlet.impl.windowings.CountWindowConfig;
import org.apache.heron.streamlet.impl.windowings.CustomWindowConfig;
import org.apache.heron.streamlet.impl.windowings.TimeWindowConfig;

/**
 * WindowConfig allows Streamlet API users to program window configuration for operations
 * that rely on windowing. Currently we only support time/count based
 * sliding/tumbling windows.
 */
public interface WindowConfig {

  /**
   * Apply this WindowConfig object to a bolt object
   * @param bolt the target bolt object
   */
  void applyTo(BaseWindowedBolt bolt);

  /**
   * This is just a dummy function to avoid WindowConfig objects to be matched with Java functional interface
   * and cause ambiguous reference compiling error. In case new virtual functions are needed in WindowConfig,
   * this dummy function can be safely removed.
   */
  void Dummy();

  /**
   * Creates a time based tumbling window of windowDuration
   * @param windowDuration the duration of the tumbling window
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig TumblingTimeWindow(Duration windowDuration) {
    return new TimeWindowConfig(windowDuration, windowDuration);
  }

  /**
   * Creates a time based sliding window with windowDuration as the window duration
   * and slideInterval as slideInterval
   * @param windowDuration The Sliding Window duration
   * @param slideInterval The sliding duration
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig SlidingTimeWindow(Duration windowDuration, Duration slideInterval) {
    return new TimeWindowConfig(windowDuration, slideInterval);
  }

  /**
   * Creates a count based tumbling window of size windowSize
   * @param windowSize the size of the tumbling window
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig TumblingCountWindow(int windowSize) {
    return new CountWindowConfig(windowSize, windowSize);
  }

  /**
   * Creates a count based sliding window with windowSize as the window countsize
   * and slideSize as slide size
   * @param windowSize The Window Count Size
   * @param slideSize The slide size
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig SlidingCountWindow(int windowSize, int slideSize) {
    return new CountWindowConfig(windowSize, slideSize);
  }

  /**
   * Creates a window based on the provided custom trigger and eviction policies
   * @param triggerPolicy The trigger policy to use
   * @param evictionPolicy The eviction policy to use
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig CustomWindow(TriggerPolicy<Tuple, ?> triggerPolicy,
                                   EvictionPolicy<Tuple, ?> evictionPolicy) {
    return new CustomWindowConfig(triggerPolicy, evictionPolicy);
  }
}
