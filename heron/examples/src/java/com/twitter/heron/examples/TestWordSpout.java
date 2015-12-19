package com.twitter.heron.examples;

import java.util.Map;
import java.util.Random;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

public class TestWordSpout extends BaseRichSpout {
  // public static Logger LOG = LoggerFactory.getLogger(TestWordSpout.class);
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
    // Utils.sleep(100);
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
