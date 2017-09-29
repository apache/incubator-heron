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

package com.twitter.heron.examples.dsl;

import java.util.Arrays;

import com.twitter.heron.dsl.Builder;
import com.twitter.heron.dsl.Config;
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.Runner;
import com.twitter.heron.dsl.WindowConfig;

/**
 * This is a topology that does simple word counts.
 * <p>
 * In this topology,
 * 1. The sentence "Mary had a little lamb" is generated over and over again.
 * 2. The flatMap stage splits the sentence into words
 * 3. The map stage 'counts' each word one at a time.
 * 4. The reduceByKeyAndWindow stage assembles a tumbling count window and
 *    computes the counts of all words grouped by word.
 */
public final class WordCountDslTopology {
  private WordCountDslTopology() {
  }

  /**
   * Main method
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException("Specify topology name");
    }

    int parallelism = 1;
    if (args.length > 1) {
      parallelism = Integer.parseInt(args[1]);
    }
    Builder builder = Builder.CreateBuilder();
    builder.newSource(() -> "Mary had a little lamb")
        .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")))
        .mapToKV((word) -> new KeyValue<>(word, 1))
        .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(10), (x, y) -> x + y)
        .log();
    Config conf = new Config();
    conf.setNumContainers(parallelism);
    Runner runner = new Runner();
    runner.run(args[0], conf, builder);
  }
}
