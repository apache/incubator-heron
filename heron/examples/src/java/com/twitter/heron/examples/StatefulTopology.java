// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.examples;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.bolt.IStatefulBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.IStatefulSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

/**
 * This is a complicated stateful topology with multiple stages, stream splitting and joining.
 * <p>
 * The DAG looks like this:
 * <p>
 * TestSentenceSpout
 * |
 * |
 * SplitBolt
 * |
 * |
 * ----------------------------------------------
 * |                    |                       |
 * |                    |                       |
 * |             DoubleTransform        TripleTransform
 * |                    |                       |
 * |                    |                       |
 * ----------------------------------------------
 * |
 * |
 * CountBolt
 */

public final class StatefulTopology {
  private StatefulTopology() {
  }

  public static class TestSentenceSpout extends BaseRichSpout implements IStatefulSpout {
    private static final long serialVersionUID = 7975718575107010220L;
    private SpoutOutputCollector collector;
    private String[] sentences;
    private long emitted;

    private State spoutState;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(Map<String, Object> conf,
                     TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
      collector = spoutOutputCollector;
      sentences = new String[]{"A A A", "B B B", "C C C", "D D D", "E E E"};
      emitted = 0;
    }

    @Override
    public void nextTuple() {
      int index = (int) (emitted++ % this.sentences.length);
      final String sentence = this.sentences[index];
      collector.emit(new Values(sentence));
    }

    @Override
    public void initState(State state) {
      System.out.println("Initializing state...");
      spoutState = state;
    }

    @Override
    public void preSave(String checkpointId) {
      System.out.println("Saving state...");
      System.out.println("Current sentence emitted count: " + emitted);
      spoutState.put("emitted", emitted);
    }
  }

  public static class SplitBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -7222458361427631320L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String[] words = input.getString(0).split(" ");
      for (String word : words) {
        collector.emit(new Values(word));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class DoubleTransform extends BaseBasicBolt {

    private static final long serialVersionUID = -2228339770717019103L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String word = input.getString(0);
      String wordDoubled = word + word;
      collector.emit(new Values(wordDoubled));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word_doubled"));
    }
  }

  public static class TripleTransform extends BaseBasicBolt {

    private static final long serialVersionUID = -6382716832099587237L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String word = input.getString(0);
      String wordTripled = word + word + word;
      collector.emit(new Values(wordTripled));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word_tripled"));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  public static class CountBolt extends BaseRichBolt implements IStatefulBolt {

    private static final long serialVersionUID = 61633079479395368L;
    private OutputCollector collector;
    private Map<String, Integer> countMap;

    // State of word count
    private State countState;

    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
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
    builder.setSpout("sentence", new TestSentenceSpout(), parallelism);
    builder.setBolt("word", new SplitBolt(), parallelism)
        .shuffleGrouping("sentence");

    // Split into two stream
    builder.setBolt("double", new DoubleTransform(), parallelism)
        .shuffleGrouping("word");
    builder.setBolt("triple", new TripleTransform(), parallelism)
        .shuffleGrouping("word");

    // Join 3 stream
    builder.setBolt("count", new CountBolt(), parallelism)
        .fieldsGrouping("double", new Fields("word_doubled"))
        .fieldsGrouping("triple", new Fields("word_tripled"))
        .fieldsGrouping("word", new Fields("word"));

    Config conf = new Config();
    conf.setNumStmgrs(parallelism);

    /*
    Set config here
    */
    // For stateful processing
    conf.put(Config.TOPOLOGY_STATEFUL, true);
    conf.put(Config.TOPOLOGY_STATEFUL_CHECKPOINT_INTERVAL, 30);

    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
