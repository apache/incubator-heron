package com.twitter.heron.integration_test.common.spout;

import java.util.Map;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;


/**
 * A spout that emit "A" and "B" continuously in order, one word every "nextTuple()" called
 */
public class ABSpout extends BaseRichSpout {
  private static final long serialVersionUID = 3233011943332591934L;
  SpoutOutputCollector collector;
  private int emitted = 0;
  private String[] toSend = new String[]{"A", "B"};

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public void open(Map<String, Object> conf,
                   TopologyContext context,
                   SpoutOutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    String word = toSend[(emitted++) % toSend.length];
    collector.emit(new Values(word));
  }
}
