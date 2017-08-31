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
package com.twitter.heron.api.windowing;

import java.util.HashMap;

public class WindowingConfigs extends HashMap<String, Object> {

  private static final long serialVersionUID = 1395902349429869055L;

  /*
   * Bolt-specific configuration for windowed bolts to specify the window length as a count of
   * number of tuples
   * in the window.
   */
  public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT = "topology.bolts.window.length"
      + ".count";
  /*
     * Bolt-specific configuration for windowed bolts to specify the window length in time duration.
     */
  public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS = "topology.bolts.window"
      + ".length.duration.ms";
  /*
     * Bolt-specific configuration for windowed bolts to specify the sliding interval as a count
     * of number of tuples.
     */
  public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT = "topology.bolts.window"
      + ".sliding.interval.count";
  /*
     * Bolt-specific configuration for windowed bolts to specify the sliding interval in time
     * duration.
     */
  public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS = "topology.bolts.window"
      + ".sliding.interval.duration.ms";
  /**
   * Bolt-specific configuration for windowed bolts to specify the name of the stream on which
   * late tuples are
   * going to be emitted. This configuration should only be used from the BaseWindowedBolt
   * .withLateTupleStream builder
   * method, and not as global parameter, otherwise IllegalArgumentException is going to be thrown.
   */
  public static final String TOPOLOGY_BOLTS_LATE_TUPLE_STREAM = "topology.bolts.late.tuple.stream";
  /**
   * Bolt-specific configuration for windowed bolts to specify the maximum time lag of the tuple
   * timestamp
   * in milliseconds. It means that the tuple timestamps cannot be out of order by more than this
   * amount.
   * This config will be effective only if {@link TimestampExtractor} is specified.
   */

  public static final String TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS = "topology.bolts.tuple"
      + ".timestamp.max.lag.ms";
  /*
     * Bolt-specific configuration for windowed bolts to specify the time interval for generating
     * watermark events. Watermark event tracks the progress of time when tuple timestamp is used.
     * This config is effective only if {@link org.apache.storm.windowing.TimestampExtractor} is
     * specified.
     */
  public static final String TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS = "topology.bolts"
      + ".watermark.event.interval.ms";
}
