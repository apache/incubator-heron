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

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.examples.streamlet.utils.StreamletUtils;
import com.twitter.heron.streamlet.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class RepartitionTopology {
  private static final Logger LOG =
      Logger.getLogger(RepartitionTopology.class.getName());

  private static List<Integer> repartitionStreamlet(int incomingInteger, int numPartitions) {
    List<Integer> partitions;

    if (incomingInteger >= 0 && incomingInteger < 25) {
      partitions = Arrays.asList(0, 1);
    } else if (incomingInteger > 26 && incomingInteger < 50) {
      partitions = Arrays.asList(2, 3);
    } else if (incomingInteger > 50 && incomingInteger < 75) {
      partitions = Arrays.asList(4, 5);
    } else if (incomingInteger > 76 && incomingInteger <= 100) {
      partitions = Arrays.asList(6, 7);
    } else {
      partitions = Arrays.asList(ThreadLocalRandom.current().nextInt(0, 8));
    }

    String logMessage = String.format("Sending value %d to partitions: %s",
        incomingInteger,
        StreamletUtils.intListAsString(partitions));

    LOG.info(logMessage);

    return partitions;
  }

  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.createBuilder();

    Streamlet<Integer> randomIntegers = processingGraphBuilder
        .newSource(() -> {
          Utils.sleep(50);
          return ThreadLocalRandom.current().nextInt(100);
        })
        .setNumPartitions(2)
        .setName("random-integer-source");

    randomIntegers
        .repartition(8, RepartitionTopology::repartitionStreamlet)
        .setName("repartition-incoming-values")
        .repartition(2)
        .setName("reduce-partitions-for-logging-operation")
        .log();

    String topologyName = StreamletUtils.getTopologyName(args);

    new Runner().run(topologyName, new Config(), processingGraphBuilder);
  }
}
