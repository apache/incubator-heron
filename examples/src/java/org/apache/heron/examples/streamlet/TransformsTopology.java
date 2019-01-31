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
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.SerializableTransformer;

/**
 * In this topology, a supplier generates an indefinite series of random integers between 1
 * and 100. From there, a series of transform operations are applied that ultimately leave
 * the original value unchanged.
 */
public final class TransformsTopology {
  private TransformsTopology() {
  }

  private static final Logger LOG = Logger.getLogger(TransformsTopology.class.getName());

  /**
   * This transformer leaves incoming values unmodified. The Consumer simply accepts incoming
   * values as-is during the transform phase.
   */
  private static class DoNothingTransformer<T> implements SerializableTransformer<T, T> {
    private static final long serialVersionUID = 3717991700067221067L;

    public void setup(Context context) {
    }

    /**
     * Here, the incoming value is accepted as-is and not changed (hence the "do nothing"
     * in the class name).
     */
    public void transform(T in, Consumer<T> consumer) {
      consumer.accept(in);
    }

    public void cleanup() {
    }
  }

  /**
   * This transformer increments incoming values by a user-supplied increment (which can also,
   * of course, be negative).
   */
  private static class IncrementTransformer implements SerializableTransformer<Integer, Integer> {
    private static final long serialVersionUID = -3198491688219997702L;
    private int increment;
    private int total;

    IncrementTransformer(int increment) {
      this.increment = increment;
    }

    public void setup(Context context) {
      context.registerMetric("InCrementMetric", 30, () -> total);
    }

    /**
     * Here, the incoming value is incremented by the value specified in the
     * transformer's constructor.
     */
    public void transform(Integer in, Consumer<Integer> consumer) {
      int incrementedValue = in + increment;
      total += increment;
      consumer.accept(incrementedValue);
    }

    public void cleanup() {
    }
  }

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder builder = Builder.newBuilder();

    /**
     * The processing graph consists of a supplier streamlet that emits
     * random integers between 1 and 100. From there, a series of transformers
     * is applied. At the end of the graph, the original value is ultimately
     * unchanged.
     */
    builder.newSource(() -> ThreadLocalRandom.current().nextInt(100))
        .transform(new DoNothingTransformer<>())
        .transform(new IncrementTransformer(10))
        .transform(new IncrementTransformer(-7))
        .transform(new DoNothingTransformer<>())
        .transform(new IncrementTransformer(-3))
        .log();

    Config config = Config.defaultConfig();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder);
  }
}
