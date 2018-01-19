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
package com.twitter.heron.examples.eco;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

@SuppressWarnings({"serial", "HiddenField"})
public class TestNameSpout extends BaseRichSpout {
  private boolean isdistributed;
  private SpoutOutputCollector collector;

  public TestNameSpout() {
    this(true);
  }

  public TestNameSpout(boolean isDistributed) {
    isdistributed = isDistributed;
  }

  public void open(Map<String, Object> conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    this.collector = collector;
  }

  public void close() {

  }

  public void nextTuple() {
    Utils.sleep(100);
    final String[] words = new String[] {"marge", "homer", "bart", "simpson", "lisa"};
    final Random rand = new Random();
    final String word = words[rand.nextInt(words.length)];
    collector.emit(new Values(word));
  }

  public void ack(Object msgId) {

  }

  public void fail(Object msgId) {

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("name"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    if (!isdistributed) {
      Map<String, Object> ret = new HashMap<String, Object>();
      ret.put(Config.TOPOLOGY_WORKERS, 1);
      return ret;
    } else {
      return null;
    }
  }
}
