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


package org.apache.heron.examples.api;

import java.util.Map;
import java.util.Random;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.common.basics.ByteAmount;

/**
 * This is a topology that does simple word counts.
 * <p>
 * In this topology,
 * 1. the spout task generate a set of random words during initial "open" method.
 * (~128k words, 20 chars per word)
 * 2. During every "nextTuple" call, each spout simply picks a word at random and emits it
 * 3. Spouts use a fields grouping for their output, and each spout could send tuples to
 * every other bolt in the topology
 * 4. Bolts maintain an in-memory map, which is keyed by the word emitted by the spouts,
 * and updates the count when it receives a tuple.
 * The difference between this topology and the StatefulWordCountTopology is that this topology
 * has some stateful components. In particular the bolt remembers the count in the
 * state variable. This way the counts are guaranteed to be accurate regardless of
 * spouts/bolt/system failures.
 */
public final class StatefulWordCountTopology {
  private StatefulWordCountTopology() {
  }

  // Utils class to generate random String at given length
  public static class RandomString {
    private final char[] symbols;

    private final Random random = new Random();

    private final char[] buf;

    public RandomString(int length) {
      // Construct the symbol set
      StringBuilder tmp = new StringBuilder();
      for (char ch = '0'; ch <= '9'; ++ch) {
        tmp.append(ch);
      }

      for (char ch = 'a'; ch <= 'z'; ++ch) {
        tmp.append(ch);
      }

      symbols = tmp.toString().toCharArray();
      if (length < 1) {
        throw new IllegalArgumentException("length < 1: " + length);
      }

      buf = new char[length];
    }

    public String nextString() {
      for (int idx = 0; idx < buf.length; ++idx) {
        buf[idx] = symbols[random.nextInt(symbols.length)];
      }

      return new String(buf);
    }
  }

  /**
   * A spout that emits a random word. Note that we dont implement
   * the IStatefulComponent since the spout does not really has any state
   * worth saving. This also illustrates that a Heron topology can
   * have some components that have empty state
   */
  public static class WordSpout extends BaseRichSpout {
    private static final long serialVersionUID = 4322775001819135036L;

    private static final int ARRAY_LENGTH = 128 * 1024;
    private static final int WORD_LENGTH = 20;

    private final String[] words = new String[ARRAY_LENGTH];

    private final Random rnd = new Random(31);

    private SpoutOutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      RandomString randomString = new RandomString(WORD_LENGTH);
      for (int i = 0; i < ARRAY_LENGTH; i++) {
        words[i] = randomString.nextString();
      }

      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      int nextInt = rnd.nextInt(ARRAY_LENGTH);
      collector.emit(new Values(words[nextInt]));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  public static class ConsumerBolt extends BaseRichBolt
         implements IStatefulComponent<String, Integer> {
    private static final long serialVersionUID = -5470591933906954522L;

    private OutputCollector collector;
    private Map<String, Integer> countMap;
    private State<String, Integer> myState;

    @Override
    public void initState(State<String, Integer> state) {
      this.myState = state;
    }

    @Override
    public void preSave(String checkpointId) {
      // Nothing really since we operate out of the system supplied state
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
      String key = tuple.getString(0);
      if (myState.get(key) == null) {
        myState.put(key, 1);
      } else {
        Integer val = myState.get(key);
        myState.put(key, ++val);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
  }

  /**
   * Main method
   */
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    if (args.length < 1) {
      throw new RuntimeException("Specify topology name");
    }

    int parallelism = 1;
    if (args.length > 1) {
      parallelism = Integer.parseInt(args[1]);
    }
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new WordSpout(), parallelism);
    builder.setBolt("consumer", new ConsumerBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));
    Config conf = new Config();
    conf.setNumStmgrs(parallelism);
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    conf.setTopologyStatefulCheckpointIntervalSecs(20);

    // configure component resources
    conf.setComponentRam("word",
        ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB));
    conf.setComponentRam("consumer",
        ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB));

    // configure container resources
    conf.setContainerDiskRequested(
        ExampleResources.getContainerDisk(2 * parallelism, parallelism));
    conf.setContainerRamRequested(
        ExampleResources.getContainerRam(4 * parallelism, parallelism));
    conf.setContainerCpuRequested(
        ExampleResources.getContainerCpu(2 * parallelism, parallelism));

    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
