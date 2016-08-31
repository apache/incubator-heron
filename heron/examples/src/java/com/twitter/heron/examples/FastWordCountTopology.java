//  Copyright 2016 Twitter. All rights reserved.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * WordCount but the spout does not stop, and the bolts are implemented in
 * java.  This can show how fast the word count can run.
 */
public class FastWordCountTopology {
  public static class FastRandomSentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1184860508880121352L;

    SpoutOutputCollector _collector;
    Random _rand;
    private static final String[] CHOICES = {
        "marry had a little lamb whos fleese was white as snow",
        "and every where that marry went the lamb was sure to go",
        "one two three four five six seven eight nine ten",
        "this is a test of the emergency broadcast system this is only a test",
        "peter piper picked a peck of pickeled peppers"
    };

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = ThreadLocalRandom.current();
    }

    @Override
    public void nextTuple() {
      String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
      _collector.emit(new Values(sentence), sentence);
    }

    @Override
    public void ack(Object id) {
      //Ignored
    }

    @Override
    public void fail(Object id) {
      _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sentence"));
    }
  }

  public static class SplitSentence extends BaseBasicBolt {
    private static final long serialVersionUID = 1184860508880121352L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String sentence = tuple.getString(0);
      for (String word: sentence.split("\\s+")) {
        collector.emit(new Values(word, 1));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static class WordCount extends BaseBasicBolt {
    private static final long serialVersionUID = 1184860508880121352L;

    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
//      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new FastRandomSentenceSpout(), 1);

    builder.setBolt("split", new SplitSentence(), 4).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 4).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();

    String name = "wc-test";
    if (args != null && args.length > 0) {
      name = args[0];
    }

    conf.setNumWorkers(1);
    StormSubmitter.submitTopology(name, conf, builder.createTopology());
  }
}