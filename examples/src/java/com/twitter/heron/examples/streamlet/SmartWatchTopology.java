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

package com.twitter.heron.examples.streamlet;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.examples.streamlet.utils.StreamletUtils;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Runner;
import com.twitter.heron.streamlet.WindowConfig;

/**
 * This topology shows an example usage of a reduce function. A source streamlet emits smart watch readings every 10
 * seconds from one of several joggers. Those readings provide  * The processing graph then converts those smart watch
 * readings to a KeyValue object, which is passed to a reduce function that calculates a per-jogger total number of feet
 * run in the last minute. The reduced value is then used to provide a per-runner average pace (feet per minute) and the
 * result is logged using a consume operation (which allows for a formatted log).
 */
public final class SmartWatchTopology {
  private SmartWatchTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(SmartWatchTopology.class.getName());

  private static final List<String> JOGGERS = Arrays.asList(
      "bill",
      "ted"
  );

  private static class SmartWatchReading implements Serializable {
    private static final long serialVersionUID = -6555650939020508026L;
    private final String joggerId;
    private final int feetRun;


    SmartWatchReading() {
      Utils.sleep(1000);
      this.joggerId = StreamletUtils.randomFromList(JOGGERS);
      this.feetRun = ThreadLocalRandom.current().nextInt(200, 400);
      LOG.info(String.format("Emitted smart watch reading: %s", this));
    }

    KeyValue<String, Integer> toKV() {
      return new KeyValue<>(joggerId, feetRun);
    }

    @Override
    public String toString() {
      return String.format("(user: %s, distance: %d)", joggerId, feetRun);
    }
  }

  private static int reduce(int cumulativeFeetRun, int incomingFeetRun) {
    return cumulativeFeetRun + incomingFeetRun;
  }

  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.createBuilder();

    processingGraphBuilder.newSource(SmartWatchReading::new)
        .setName("incoming-watch-readings")
        .mapToKV(SmartWatchReading::toKV)
        .setName("map-smart-watch-readings-to-kv")
        .reduceByKeyAndWindow(
            // The time window (1 minute of clock time)
            WindowConfig.TumblingTimeWindow(Duration.ofSeconds(10)),
            // The optional "identity" value that acts as a starting point for the reduce function. This is
            // returned if the reduce function is provided with no values to work with inside of the time
            // window.
            0,
            // The reduce function (produces a cumulative sum)
            SmartWatchTopology::reduce
        )
        .setName("reduce-to-total-distance-per-jogger")
        .map(kw -> {
          // The per-key result of the previous reduce step
          long totalFeetRun = kw.getValue();
          LOG.info("Total feet run " + totalFeetRun);
          // The amount of time elapsed
          long startTime = kw.getKey().getWindow().getStartTime();
          LOG.info("Start time " + startTime);
          long endTime = kw.getKey().getWindow().getEndTime();
          LOG.info("End time " + endTime);
          long timeLengthMillis = endTime - startTime; // Cast to float to use as denominator
          LOG.info("Time length " + timeLengthMillis);
          float feetPerMinute = totalFeetRun / (float) (timeLengthMillis / 1000);
          LOG.info("Feet per minute " + feetPerMinute);
          // Reduce to two decimal places
          String paceString = new DecimalFormat("#.##").format(feetPerMinute);
          LOG.info("As decimal " + paceString);
          // Return a per-jogger average pace
          return new KeyValue<>(kw.getKey().getKey(), paceString);
        })
        .setName("calculate-average-speed")
        .consume(kv -> LOG.info(String.format("(runner: %s, avgFeetPerMinute: %s)", kv.getKey(), kv.getValue())));

    Config config = new Config();

    String topologyName = StreamletUtils.getTopologyName(args);

    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}