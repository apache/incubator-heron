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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;

/**
 * This topology demonstrates the usage of a simple repartitioning algorithm
 * using the Heron Streamlet API for Java. Normally, streamlet elements are
 * distributed randomly across downstream instances when processed.
 * Repartitioning enables you to select which instances (partitions) to send
 * elements to on the basis of a user-defined logic. Here, a source streamlet
 * emits an indefinite series of random integers between 1 and 100. The value
 * of that number then determines to which topology instance (partition) the
 * element is routed.
 */
public final class RepartitionTopology {
  private RepartitionTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(RepartitionTopology.class.getName());

  /**
   * The repartition function that determines to which partition each incoming
   * streamlet element is routed (across 8 possible partitions). Integers between 1
   * and 24 are routed to partitions 0 and 1, integers between 25 and 40 to partitions
   * 2 and 3, and so on.
   */
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

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.newBuilder();

    Streamlet<Integer> randomIntegers = processingGraphBuilder
        .newSource(() -> {
          // Random integers are emitted every 50 milliseconds
          StreamletUtils.sleep(50);
          return ThreadLocalRandom.current().nextInt(100);
        })
        .setNumPartitions(2)
        .setName("random-integer-source");

    randomIntegers
        // The specific repartition logic is applied here
        .repartition(8, RepartitionTopology::repartitionStreamlet)
        .setName("repartition-incoming-values")
        // Here, a generic repartition logic is applied (simply
        // changing the number of partitions without specifying
        // how repartitioning will take place)
        .repartition(2)
        .setName("reduce-partitions-for-logging-operation")
        .log();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    Config config = Config.defaultConfig();

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
