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
import java.util.Random;

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
import com.twitter.heron.api.utils.Utils;

/**
 * This is a complicated stateful topology with multiple stages, stream splitting and joining.
 * The TestSentenceSpout would produce fixed amount of tuples and then do nothing.
 * So the spout would have a deterministic and finite input, and we could compare the actual result
 * with expected result for correctness verification.
 * During nextTuple(), the spout would randomly throw exceptions,
 * when # of emitted tuples is a multiple of EXCEPTION_THROWING_INTERVAL_COUNT
 * - 1. 100% throw exception when emitted tuples equal to EXCEPTION_THROWING_INTERVAL_COUNT
 * so it is guaranteed the instance would restart at least once.
 * - 2. EXCEPTION_PROBABILITY to throw exception otherwise
 * The state in CountBolt should be deterministic if exactly once failure recovery is guaranteed.
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

    private static final int EMIT_INTERVAL_MS = 1;
    private static final long TOTAL_COUNT_TO_EMIT = 200 * 1000;
    private static final long EXCEPTION_THROWING_INTERVAL_COUNT = TOTAL_COUNT_TO_EMIT / 10;
    private static final double EXCEPTION_PROBABILITY = 0.2;
    private static final String KEY_EMITTED = "tuples_emitted";
    // This value will be restored from the spoutState
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
    }

    @Override
    public void nextTuple() {
      // The spout would emit only finite # of tuples
      if (emitted >= TOTAL_COUNT_TO_EMIT) {
        System.out.println("Done the emit. Sleep for a while...");
        Utils.sleep(2000);
        return;
      }

      // Randomly throw exceptions when # of emitted tuples is a multiple of
      // EXCEPTION_THROWING_INTERVAL_COUNT
      // 1. 100% throw exception when emitted tuples equal to EXCEPTION_THROWING_INTERVAL_COUNT
      // so it is guaranteed to have at least one exception
      // 2. EXCEPTION_PROBABILITY to throw exception otherwise
      if (emitted % EXCEPTION_THROWING_INTERVAL_COUNT == 0) {
        if (emitted == EXCEPTION_THROWING_INTERVAL_COUNT
            || new Random().nextDouble() < EXCEPTION_PROBABILITY) {
          throw new RuntimeException("Intentional exception for failure recovery testing.");
        }
      }

      int index = (int) (emitted++ % this.sentences.length);
      final String sentence = this.sentences[index];
      collector.emit(new Values(sentence));

      // Sleep a while for rate control
      Utils.sleep(EMIT_INTERVAL_MS);
    }

    @Override
    public void initState(State state) {
      System.out.println("Initializing state...");
      spoutState = state;

      // Restore the value of emitted
      emitted = spoutState.containsKey(KEY_EMITTED) ?
          (long) spoutState.get(KEY_EMITTED) : 0;
      System.out.println("Recover from last state.. Have emitted tuples: " + emitted);
    }

    @Override
    public void preSave(String checkpointId) {
      System.out.println("Saving state...");
      System.out.println("Current sentence emitted count: " + emitted);
      spoutState.put(KEY_EMITTED, emitted);
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
