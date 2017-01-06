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
//  limitations under the License

package com.twitter.heron.examples;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.IStatefulBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.common.basics.ByteAmount;

/**
 * This is a topology that does simple word counts.
 * <p>
 * The ConsumerBolt is stateful. It will checkpoint its word-count result at an interval.
 */
public final class StatefulWordCountTopology {
  private StatefulWordCountTopology() {
  }

  public static class TestWordSpout extends BaseRichSpout {

    private static final long serialVersionUID = -3217886193225455451L;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;

    public void open(
        Map<String, Object> conf,
        TopologyContext context,
        SpoutOutputCollector acollector) {
      collector = acollector;
      words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
      rand = new Random();
    }

    public void close() {
    }

    public void nextTuple() {
      final String word = words[rand.nextInt(words.length)];
      collector.emit(new Values(word));
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  public static class ConsumerBolt extends BaseRichBolt implements IStatefulBolt {
    private static final long serialVersionUID = -5470591933906954522L;

    private OutputCollector collector;
    private Map<String, Integer> countMap;

    // State of word count
    private State countState;

    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      System.out.println("Preparing...");
      collector = outputCollector;
      countMap = new HashMap<String, Integer>();

      // Initialize the value of word count
      for (Map.Entry<Serializable, Serializable> entry : countState.entrySet()) {
        if (entry.getKey() instanceof String && entry.getValue() instanceof Integer) {
          countMap.put((String) entry.getKey(), (Integer) entry.getValue());
        }
      }
    }

    @Override
    public void execute(Tuple tuple) {
      String key = tuple.getString(0);
      if (countMap.get(key) == null) {
        countMap.put(key, 1);
      } else {
        Integer val = countMap.get(key);
        countMap.put(key, ++val);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void initState(State state) {
      System.out.println("Initializing state...");
      countState = state;
    }

    @Override
    public void preSave(String checkpointId) {
      System.out.println("Saving state...");
      System.out.println("Current word count result: ");
      System.out.println(countMap.toString());
      for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
        countState.put(entry.getKey(), entry.getValue());
      }
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
    builder.setSpout("word", new TestWordSpout(), parallelism);
    builder.setBolt("consumer", new ConsumerBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));
    Config conf = new Config();
    conf.setNumStmgrs(parallelism);

    /*
    Set config here
    */
    conf.setComponentRam("word", ByteAmount.fromGigabytes(2));
    conf.setComponentRam("consumer", ByteAmount.fromGigabytes(3));
    conf.setContainerCpuRequested(6);

    // For stateful processing
    conf.put(Config.TOPOLOGY_COMPONENT_STATEFUL, true);
    conf.put(Config.TOPOLOGY_STATEFUL_CHECKPOINT_INTERVAL, 30);

    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
