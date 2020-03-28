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
import java.util.logging.Logger;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;

/**
 * This topology is an implementation of a simple topology using the
 * Streamlet API. CPU and ram are configured for each component.
 */
public final class ComponentConfigTopology {
  private ComponentConfigTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(ComponentConfigTopology.class.getName());

  private static final List<String> SENTENCES = Arrays.asList(
      "I have nothing to declare but my genius",
      "You can even",
      "Compassion is an action word with no boundaries",
      "To thine own self be true"
  );

  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.newBuilder();

    processingGraphBuilder
        // The origin of the processing graph: an indefinite series of sentences chosen
        // from the list
        .newSource(() -> StreamletUtils.randomFromList(SENTENCES))
        .setName("random-sentences-source")
        // Each sentence is "flattened" into a Streamlet<String> of individual words
        .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
        .setName("flatten-into-individual-words")
        // The final output is logged using a user-supplied format
        .consume(w -> {
          String logMessage = String.format("(word: %s)", w);
          LOG.info(logMessage);
        })
        .setName("consumer");

    // The topology's parallelism (the number of containers across which the topology's
    // processing instance will be split) can be defined via the second command-line
    // argument (or else the default of 2 will be used).
    int topologyParallelism = StreamletUtils.getParallelism(args, 2);

    Config config = Config.newBuilder()
        .setNumContainers(topologyParallelism)
        .setPerContainerCpu(1)
        .build();

    config.getHeronConfig().setComponentCpu("random-sentences-source", 0.3);
    config.getHeronConfig().setComponentRam("random-sentences-source",
        ByteAmount.fromMegabytes(300));
    config.getHeronConfig().setComponentCpu("flatten-into-individual-words", 0.3);
    config.getHeronConfig().setComponentRam("flatten-into-individual-words",
        ByteAmount.fromMegabytes(300));
    config.getHeronConfig().setComponentCpu("consumer", 0.2);
    config.getHeronConfig().setComponentRam("consumer", ByteAmount.fromMegabytes(200));

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
