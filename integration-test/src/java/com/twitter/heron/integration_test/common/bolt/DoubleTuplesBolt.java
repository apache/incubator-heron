package com.twitter.heron.integration_test.common.bolt;


import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

/**
 * This bolt will doubly emit what it receives.
 */

public class DoubleTuplesBolt extends BaseBasicBolt {
  Fields _fields;

  public DoubleTuplesBolt(Fields fields) {
    _fields = fields;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    collector.emit(input.getValues());
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(_fields);
  }
}
