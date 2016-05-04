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
 * A bolt to count how many different words
 */

public class CountAggregatorBolt<T> extends BaseBatchBolt<T> {
  private static final long serialVersionUID = 590728128451229945L;
  OutputCollector collector;
  int sum = 0;

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    sum += tuple.getInteger(0);
  }

  @Override
  public void finishBatch() {
    collector.emit(new Values(sum));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sum"));
  }
}
