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


package org.apache.heron.examples.streamlet;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.WindowConfig;

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
      StreamletUtils.sleep(1000);
      this.joggerId = StreamletUtils.randomFromList(JOGGERS);
      this.feetRun = ThreadLocalRandom.current().nextInt(200, 400);
    }

    String getJoggerId() {
      return joggerId;
    }

    int getFeetRun() {
      return feetRun;
    }
  }

  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.newBuilder();

    processingGraphBuilder.newSource(SmartWatchReading::new)
        .setName("incoming-watch-readings")
        .reduceByKeyAndWindow(
            // Key extractor
            reading -> reading.getJoggerId(),
            // Value extractor
            reading -> reading.getFeetRun(),
            // The time window (1 minute of clock time)
            WindowConfig.TumblingTimeWindow(Duration.ofSeconds(10)),
            // The reduce function (produces a cumulative sum)
            (cumulative, incoming) -> cumulative + incoming
        )
        .setName("reduce-to-total-distance-per-jogger")
        .map(keyWindow -> {
          // The per-key result of the previous reduce step
          long totalFeetRun = keyWindow.getValue();

          // The amount of time elapsed
          long startTime = keyWindow.getKey().getWindow().getStartTime();
          long endTime = keyWindow.getKey().getWindow().getEndTime();
          long timeLengthMillis = endTime - startTime; // Cast to float to use as denominator

          // The feet-per-minute calculation
          float feetPerMinute = totalFeetRun / (float) (timeLengthMillis / 1000);

          // Reduce to two decimal places
          String paceString = new DecimalFormat("#.##").format(feetPerMinute);

          // Return a per-jogger average pace
          return new KeyValue<>(keyWindow.getKey().getKey(), paceString);
        })
        .setName("calculate-average-speed")
        .consume(kv -> {
          String logMessage = String.format("(runner: %s, avgFeetPerMinute: %s)",
              kv.getKey(),
              kv.getValue());

          LOG.info(logMessage);
        });

    Config config = Config.defaultConfig();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
