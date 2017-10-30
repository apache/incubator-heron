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

public class WordCountStreamletTopology {
    private static final int DEFAULT_PARALLELISM = 2;

    private static final List<String> SENTENCES = Arrays.asList(
            "I have nothing to declare but my genius",
            "All work and no play makes Jack a dull boy",
            "Wherefore art thou Romeo?",
            "Houston we have a problem"
    );

    // This is the reduce function that will create a word count within each
    // time window. In reduce functions, the first argument is always some cumulative
    // figure from all computations in the window thus far, whereas the second
    // argument is the incoming value.
    private static int reduce(int cumulative, int incoming) {
        return cumulative + incoming;
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        processingGraphBuilder
                // The graph begins with an unbounded series of sentences chosen at random
                // from a pre-selected list
                .newSource(() -> StreamletUtils.randomFromList(SENTENCES))
                // Each sentence is then "flatted" into a list of individual words
                .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")))
                // Each word is converted into a key-value where the key is the word
                // and the value is the count (in this example, each word can only
                // occur once in a given sentence.
                .mapToKV((word) -> new KeyValue<>(word, 1))
                // A count is generated across each tumbling count window of 10
                // computations. The reduce function simply sums all the count
                // values together to produce the count within that window.
                .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(10), WindowedWordCount::reduce)
                // Finally, the count is logged
                .log();

        Config config = new Config();

        // Applies the default parallelism of 2 unless a different number if supplied
        // via the second CLI argument
        int parallelism = StreamletUtils.getParallelism(args, DEFAULT_PARALLELISM);
        config.setNumContainers(parallelism);

        // Fetch the topology name from the first CLI argument
        String topologyName = StreamletUtils.getTopologyName(args);

        // Finally, convert the processing graph and configuration into a Heron topology
        // and run it in a Heron cluster.
        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}
