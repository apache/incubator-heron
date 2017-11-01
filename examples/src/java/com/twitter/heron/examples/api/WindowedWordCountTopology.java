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
package com.twitter.heron.examples.api;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BaseWindowedBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;

public final class WindowedWordCountTopology {

  private WindowedWordCountTopology() {
  }

  private static class SentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 2879005791639364028L;
    private SpoutOutputCollector collector;

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
      collector.emit(new Values("Mary had a little lamb"));
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
    private Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    @SuppressWarnings("HiddenField")
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
        collector) {
      this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
      int sum = counts.get("sum");
      for (Tuple tuple : inputWindow.get()) {
        sum += tuple.getIntegerByField("value");
      }
      counts.put("sum", sum);
      collector.emit(new Values(sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sum"));
    }
  }

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

    int parallelism = 1;
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("sentence", new SentenceSpout(), parallelism);
    builder.setBolt("split", new SplitSentence(), parallelism).shuffleGrouping("sentence");
    builder.setBolt("consumer", new WindowSumBolt()
        .withWindow(BaseWindowedBolt.Count.of(10)), parallelism)
        .fieldsGrouping("split", new Fields("word"));
    Config conf = new Config();

    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
