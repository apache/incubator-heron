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

import java.util.concurrent.ThreadLocalRandom;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;

/**
 * This is a very simple topology that shows a series of streamlet operations
 * on a source streamlet of random integers (between 1 and 10). First, 1 is added
 * to each integer. That streamlet is then united with a streamlet that consists
 * of an indefinite stream of zeroes. At that point, all 2s are excluded from the
 * streamlet. The final output of the processing graph is then logged.
 */
public final class IntegerProcessingTopology {
  private IntegerProcessingTopology() {
  }

  // Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder builder = Builder.newBuilder();

    Streamlet<Integer> zeroes = builder.newSource(() -> 0);

    builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
        .setName("random-ints")
        .map(i -> i + 1)
        .setName("add-one")
        .union(zeroes)
        .setName("unify-streams")
        .filter(i -> i != 2)
        .setName("remove-twos")
        .log();

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .build();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder);
  }
}
