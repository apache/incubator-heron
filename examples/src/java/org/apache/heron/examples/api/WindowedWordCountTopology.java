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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;

public final class WindowedWordCountTopology {

  private WindowedWordCountTopology() {
  }

  private static class SentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 2879005791639364028L;
    private SpoutOutputCollector collector;
    private Random rand = new Random();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    @SuppressWarnings({"rawtypes", "HiddenField"})
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

      String[] sentences = {
          "Mary had a little lamb",
          "The quick brown fox jumps over the lazy dog",
          "The book is in front of the table",
          "Mary plays the piano"
      };

      int n = rand.nextInt(sentences.length);
      collector.emit(new Values(sentences[n]));
    }
  }

  public static class SplitSentence extends BaseBasicBolt {

    private static final long serialVersionUID = 2223204156371570768L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      for (String word : input.getString(0).split("\\s+")) {
        collector.emit(new Values(word));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  private static class WindowSumBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 8458595466693183050L;
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

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

    int parallelism = 1;
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("sentence", new SentenceSpout(), parallelism);
    builder.setBolt("split", new SplitSentence(), parallelism).shuffleGrouping("sentence");
    builder.setBolt("consumer", new WindowSumBolt()
        .withWindow(BaseWindowedBolt.Count.of(10000), BaseWindowedBolt.Count.of(5000)), parallelism)
        .fieldsGrouping("split", new Fields("word"));
    Config conf = new Config();
    conf.setMaxSpoutPending(1000000);

    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
