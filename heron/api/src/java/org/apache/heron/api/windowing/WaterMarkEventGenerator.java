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

package org.apache.heron.api.windowing;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;

/**
 * Tracks tuples across input streams and periodically emits watermark events.
 * Watermark event timestamp is the minimum of the latest tuple timestamps
 * across all the input streams (minus the lag). Once a watermark event is emitted
 * any tuple coming with an earlier timestamp can be considered as late events.
 */
public class WaterMarkEventGenerator<T extends Serializable> {
  private final Map<TopologyAPI.StreamId, Long> streamToTs;
  private final WindowManager<T> windowManager;
  private long watermarkIntervalMs;
  private final int eventTsLag;
  private final Set<TopologyAPI.StreamId> inputStreams;
  private  Map<String, Object> topoConf;
  private volatile long lastWaterMarkTs;

  /**
   * Creates a new WatermarkEventGenerator.
   *
   * @param windowManager The window manager this generator will submit watermark events to
   * interval
   * @param watermarkIntervalMs the interval at which watermarks should be emitted
   * @param eventTsLagMs The max allowed lag behind the last watermark event before an event is
   * considered late
   * @param inputStreams The input streams this generator is expected to handle
   * @param topoConf topology configurations
   */
  public WaterMarkEventGenerator(WindowManager<T> windowManager, long watermarkIntervalMs,
                                 int eventTsLagMs,
                                 Set<TopologyAPI.StreamId> inputStreams,
                                 Map<String, Object> topoConf) {
    streamToTs = new ConcurrentHashMap<>();
    this.windowManager = windowManager;
    this.watermarkIntervalMs = watermarkIntervalMs;
    this.eventTsLag = eventTsLagMs;
    this.inputStreams = inputStreams;
    this.topoConf = topoConf;
  }

  /**
   * Tracks the timestamp of the event in the stream, returns
   * true if the event can be considered for processing or
   * false if its a late event.
   */
  public boolean track(TopologyAPI.StreamId stream, long ts) {
    Long currentVal = streamToTs.get(stream);
    if (currentVal == null || ts > currentVal) {
      streamToTs.put(stream, ts);
    }
    return ts >= lastWaterMarkTs;
  }

  public void run() {
    long waterMarkTs = computeWaterMarkTs();
    if (waterMarkTs > lastWaterMarkTs) {
      this.windowManager.add(new WaterMarkEvent<>(waterMarkTs));
      lastWaterMarkTs = waterMarkTs;
    }
  }

  /**
   * Computes the min ts across all streams.
   */
  private long computeWaterMarkTs() {
    long ts = 0;
    // only if some data has arrived on each input stream
    if (streamToTs.size() >= inputStreams.size()) {
      ts = Long.MAX_VALUE;
      for (Map.Entry<TopologyAPI.StreamId, Long> entry : streamToTs.entrySet()) {
        ts = Math.min(ts, entry.getValue());
      }
    }
    return ts - eventTsLag;
  }

  public void start() {
    Config.registerTopologyTimerEvents(
        topoConf,
        "WaterMarkEventGeneratorTimer",
        Duration.ofMillis(watermarkIntervalMs),
        () -> run()
    );
  }
}
