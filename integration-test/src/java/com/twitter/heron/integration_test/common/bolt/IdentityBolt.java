package com.twitter.heron.integration_test.common.bolt;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

public class IdentityBolt extends BaseBasicBolt {
  Fields _fields;

  public IdentityBolt(Fields fields) {
    _fields = fields;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(_fields);
  }
}
