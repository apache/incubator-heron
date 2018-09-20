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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;
import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.WindowConfig;

/**
 * This topology is an implementation of the classic word count example
 * for the Heron Streamlet API for Java. A source streamlet emits an
 * indefinite series of sentences chosen at random from a pre-defined list.
 * Each sentence is then "flattened" into a list of individual words. The words
 * are then counted with a Bolt implemented with low level API. The result
 * is then logged.
 */
public final class WordCountWithBoltTopology {
  private WordCountWithBoltTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(WordCountWithBoltTopology.class.getName());

  private static final List<String> SENTENCES = Arrays.asList(
      "I have nothing to declare but my genius",
      "You can even",
      "Compassion is an action word with no boundaries",
      "To thine own self be true"
  );

  private static class WindowedSumBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    @SuppressWarnings("HiddenField")
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
        collector) {
      this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
      Map<String, Integer> counts = new HashMap<String, Integer>();

      for (Tuple tuple : inputWindow.get()) {
        String word = tuple.getStringByField("word");
        if (!counts.containsKey(word)) {
          counts.put(word, 0);
        }
        int previousCount = counts.get(word);
        counts.put(word, previousCount + 1);
      }

      System.out.println("Word Counts for window: " + counts);
    }
  }

  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.newBuilder();

    BaseWindowedBolt bolt = new WindowedSumBolt()
        .withWindow(BaseWindowedBolt.Count.of(10000), BaseWindowedBolt.Count.of(5000));

    processingGraphBuilder
        // The origin of the processing graph: an indefinite series of sentences chosen
        // from the list
        .newSource(() -> StreamletUtils.randomFromList(SENTENCES))
        .setName("random-sentences-source")
        // Each sentence is "flattened" into a Streamlet<String> of individual words
        .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
        .setName("flatten-into-individual-words")
        // The reduce operation performs the per-key (i.e. per-word) sum within each time window
        .applyBolt(bolt)
        .setName("window-sum-bolt")
        // The final output is logged using a user-supplied format
        .consume(v -> {
          String logMessage = String.format("(result: %s",
              v.toString()
          );
          LOG.info(logMessage);
        });

    // The topology's parallelism (the number of containers across which the topology's
    // processing instance will be split) can be defined via the second command-line
    // argument (or else the default of 2 will be used).
    int topologyParallelism = StreamletUtils.getParallelism(args, 2);

    Config config = Config.newBuilder()
        .setNumContainers(topologyParallelism)
        .build();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
