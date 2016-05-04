package com.twitter.heron.integration_test.common.bolt;

import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.integration_test.core.BaseBatchBolt;


/**
 * A bolt will increase the field "received" when executes once.
 * And it finally will emit the number of received tuples
 */
public class IncrementBolt<T> extends BaseBatchBolt<T> {
  private static final long serialVersionUID = 1094032885033452863L;
  OutputCollector collector;
  int received = 0;

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    received++;
  }

  @Override
  public void finishBatch() {
    collector.emit(new Values(received));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("count"));
  }
}
