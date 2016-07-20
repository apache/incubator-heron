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

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {
  private static final long serialVersionUID = 1L;
  private SpoutOutputCollector spoutOutCollector;
  private Random random;

  @Override
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    spoutOutCollector = collector;
    random = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    String[] sentences = new String[] {"the cow jumped over the moon",
        "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs",
        "i am at two with nature"};
    String sentence = sentences[random.nextInt(sentences.length)];
    spoutOutCollector.emit(new Values(sentence));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  public SpoutOutputCollector getSpoutOutCollector() {
    return spoutOutCollector;
  }

  public void setSpoutOutCollector(SpoutOutputCollector spoutOutCollector) {
    this.spoutOutCollector = spoutOutCollector;
  }

  public Random getRandom() {
    return random;
  }

  public void setRandom(Random random) {
    this.random = random;
  }

}
