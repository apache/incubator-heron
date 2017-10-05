// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.api.windowing;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.twitter.heron.api.generated.TopologyAPI;

/**
 * Tracks tuples across input streams and periodically emits watermark events.
 * Watermark event timestamp is the minimum of the latest tuple timestamps
 * across all the input streams (minus the lag). Once a watermark event is emitted
 * any tuple coming with an earlier timestamp can be considered as late events.
 */
public class WaterMarkEventGenerator<T> {
  private final WindowManager<T> windowManager;
  private final int eventTsLag;
  private final Set<TopologyAPI.StreamId> inputStreams;
  private final Map<TopologyAPI.StreamId, Long> streamToTs;
  private volatile long lastWaterMarkTs;
  private boolean started = false;

  /**
   * Creates a new WatermarkEventGenerator.
   *
   * @param windowManager The window manager this generator will submit watermark events to
   * interval
   * @param eventTsLagMs The max allowed lag behind the last watermark event before an event is
   * considered late
   * @param inputStreams The input streams this generator is expected to handle
   */
  public WaterMarkEventGenerator(WindowManager<T> windowManager, int eventTsLagMs,
                                 Set<TopologyAPI.StreamId> inputStreams) {
    this.windowManager = windowManager;
    streamToTs = new ConcurrentHashMap<>();
    this.eventTsLag = eventTsLagMs;
    this.inputStreams = inputStreams;
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
    if (started) {
      long waterMarkTs = computeWaterMarkTs();
      if (waterMarkTs > lastWaterMarkTs) {
        this.windowManager.add(new WaterMarkEvent<>(waterMarkTs));
        lastWaterMarkTs = waterMarkTs;
      }
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
    started = true;
  }
}
