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

import com.twitter.heron.examples.streamlet.utils.StreamletUtils;
import com.twitter.heron.streamlet.*;

import java.util.Arrays;
import java.util.List;

/**
 * This topology uses the Heron Streamlet API for Java to implement
 * the classic word count example for stream processing.
 */
public class WordCountStreamletTopology {
  /**
   * The default parallelism (the number of containers across which topology processing
   * instances are distributed) for this topology, which can be overridden using
   * command-line arguments.
   */
  private static final int DEFAULT_PARALLELISM = 2;

  /**
   * A list of sentences that will be chosen at random to create the source streamlet
   * for the processing graph.
   */
  private static final List<String> SENTENCES = Arrays.asList(
      "I have nothing to declare but my genius",
      "All work and no play makes Jack a dull boy",
      "Wherefore art thou Romeo?",
      "Houston we have a problem"
  );

  /**
   * This reduce function will count how many times each word is encountered
   * within the specified time window. In reduce functions in the Streamlet API,
   * the first argument is always some cumulative value for all computations thus
   * far, while the second argument is the incoming value.
   */
  private static int reduce(int cumulative, int incoming) {
    return cumulative + incoming;
  }

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.createBuilder();

    processingGraphBuilder
        /**
         * The processing graph begins with a source streamlet consisting of an
         * unbounded series of sentences chosen at random from the the pre-selected
         * SENTENCES list above.
         */
        // The graph begins with an unbounded series of sentences chosen at random
        .newSource(() -> StreamletUtils.randomFromList(SENTENCES))
        /**
         * Each sentence is "flattened" into a list of individual words,
         * producing a new streamlet.
         */
        .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")))
        /**
         * Each word in the streamlet is converted into a KeyValue object in
         * which the key is the word and the value is the count (in this example,
         * each word can only occur once in a given sentence, so the value is
         * always 1).
         */
        .mapToKV((word) -> new KeyValue<>(word, 1))
        /**
         * A count is generated across each tumbling count window of 10
         * computations. The reduce function simply sums all the count
         * values together to produce the count within that window.
         */
        .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(10),
                WordCountStreamletTopology::reduce)
        /**
         * Finally, the word/count KeyValue objects are logged.
         */
        .log();

    Config config = new Config();

    /**
     * Applies the default parallelism of 2 unless a different number is supplied
     * via the second command-line argument.
     */
    int parallelism = StreamletUtils.getParallelism(args, DEFAULT_PARALLELISM);
    config.setNumContainers(parallelism);

    /**
     * Fetches the topology name from the first command-line argument
     */
    String topologyName = StreamletUtils.getTopologyName(args);

    /**
     * Finally, the processing graph and configuration are passed to the Runner,
     * which converts the graph into a Heron topology that can be run in a Heron
     * cluster.
     */
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
