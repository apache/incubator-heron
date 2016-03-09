package com.twitter.heron.examples;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestWordSpout extends BaseRichSpout {
  boolean _isDistributed;
  SpoutOutputCollector _collector;
  String[] words;
  Random rand;

  public TestWordSpout() {
    this(true);
  }

  public TestWordSpout(boolean isDistributed) {
    _isDistributed = isDistributed;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
    rand = new Random();
  }

  public void close() {
  }

  public void nextTuple() {
    final String word = words[rand.nextInt(words.length)];
    _collector.emit(new Values(word));
  }

  public void ack(Object msgId) {
  }

  public void fail(Object msgId) {
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
