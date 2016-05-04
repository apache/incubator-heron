package com.twitter.heron.integration_test.common.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.integration_test.core.BaseBatchBolt;


/**
 * CounBolt will count the number different words received, and finally output the number
 */
public class WordCountBolt<T> extends BaseBatchBolt<T> {
  public static final Logger LOG = Logger.getLogger(WordCountBolt.class.getName());
  private static final long serialVersionUID = -7592911369781228601L;
  OutputCollector collector;
  HashMap<String, Integer> cache = new HashMap<String, Integer>();

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    // Insert it into the cache
    String word = tuple.getString(0);
    Integer count = cache.get(word);
    if (count == null) {
      count = 1;
    } else {
      count++;
    }
    LOG.info("Counter Value = " + count);
    cache.put(word, count);
  }

  @Override
  public void finishBatch() {
    collector.emit(new Values(cache.size()));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("count"));
  }
}
