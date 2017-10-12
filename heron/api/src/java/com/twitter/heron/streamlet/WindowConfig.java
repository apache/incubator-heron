//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.streamlet;


import java.time.Duration;

import com.twitter.heron.streamlet.impl.WindowConfigImpl;

/**
 * WindowConfig allows Streamlet API users to program window configuration for operations
 * that rely on windowing. Currently we only support time/count based
 * sliding/tumbling windows.
 */
public interface WindowConfig {
  /**
   * Creates a time based tumbling window of windowDuration
   * @param windowDuration the duration of the tumbling window
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig TumblingTimeWindow(Duration windowDuration) {
    return new WindowConfigImpl(windowDuration, windowDuration);
  }

  /**
   * Creates a time based sliding window with windowDuration as the window duration
   * and slideInterval as slideInterval
   * @param windowDuration The Sliding Window duration
   * @param slideInterval The sliding duration
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig SlidingTimeWindow(Duration windowDuration, Duration slideInterval) {
    return new WindowConfigImpl(windowDuration, slideInterval);
  }

  /**
   * Creates a count based tumbling window of size windowSize
   * @param windowSize the size of the tumbling window
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig TumblingCountWindow(int windowSize) {
    return new WindowConfigImpl(windowSize, windowSize);
  }

  /**
   * Creates a count based sliding window with windowSize as the window countsize
   * and slideSize as slide size
   * @param windowSize The Window Count Size
   * @param slideSize The slide size
   * @return WindowConfig that can be passed to the transformation
   */
  static WindowConfig SlidingCountWindow(int windowSize, int slideSize) {
    return new WindowConfigImpl(windowSize, slideSize);
  }
}
