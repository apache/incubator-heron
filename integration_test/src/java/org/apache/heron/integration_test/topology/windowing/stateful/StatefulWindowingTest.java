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

package org.apache.heron.integration_test.topology.windowing.stateful;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BaseStatefulWindowedBolt;
import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.api.windowing.TupleWindow;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.core.TestTopologyBuilder;

public final class StatefulWindowingTest extends AbstractTestTopology {

  protected StatefulWindowingTest(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder builder) {

    builder.setSpout("sentence", new RandomSentenceSpout(), 1);
    builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("sentence");
    builder.setBolt("consumer", new ConsumerBolt()
        .withWindow(BaseWindowedBolt.Count.of(10), BaseWindowedBolt.Count.of(10)), 1)
        .fieldsGrouping("split", new Fields("word"));

    return builder;
  }

  /**
   * Main method
   */
  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException, MalformedURLException {
    StatefulWindowingTest statefulWindowingTest = new StatefulWindowingTest(args);
    Config conf = new Config();
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    statefulWindowingTest.submit(conf);
  }

  /**
   * A spout that emits a random word
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static class RandomSentenceSpout extends BaseRichSpout
      implements IStatefulComponent<String, Integer> {
    private static final long serialVersionUID = 4322775001819135036L;

    private SpoutOutputCollector collector;

    private State<String, Integer> myState;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      Utils.sleep(100);
      String[] sentences = new String[]{
          sentence("the cow jumped over the moon"),
          sentence("an apple a day keeps the doctor away"),
          sentence("four score and seven years ago"),
          sentence("snow white and the seven dwarfs"),
          sentence("i am at two with nature")};
      System.out.println("myState: " + myState);
      final String sentence = sentences[myState.get("count") % sentences.length];

      System.out.println("Emitting tuple: " + sentence);

      collector.emit(new Values(sentence));
      myState.put("count", myState.get("count") + 1);
    }

    protected String sentence(String input) {
      return input;
    }

    @Override
    public void initState(State<String, Integer> state) {
      this.myState = state;
      myState.put("count", 0);

      System.out.println("initState: " + myState);
    }

    @Override
    public void preSave(String checkpointId) {

    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static class ConsumerBolt
      extends BaseStatefulWindowedBolt<Integer, HashMap<String, Integer>> {
    private static final long serialVersionUID = -5470591933906954522L;

    private OutputCollector collector;
    private State myState;
    private int windowCount = 0;

    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("state"));
    }

    @Override
    public void initState(State<Integer, HashMap<String, Integer>> state) {
      System.out.println("init state: " + state);
      this.myState = state;
    }

    @Override
    public void preSave(String checkpointId) {

    }

    @Override
    public void execute(TupleWindow inputWindow) {

      HashMap<String, Integer> wordCount = new HashMap<>();
      for (Tuple tuple : inputWindow.get()) {
        String key = tuple.getString(0);
        if (wordCount.get(key) == null) {
          wordCount.put(key, 1);
        } else {
          Integer val = wordCount.get(key);
          wordCount.put(key, ++val);
        }
      }
      myState.put(windowCount, wordCount);
      windowCount++;
      System.out.println("STATE: " + myState);

      JSONObject jsonObject = new JSONObject();
      jsonObject.putAll(myState);
      collector.emit(new Values(jsonObject.toJSONString()));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static class SplitBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 7017201816670732270L;

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
}
