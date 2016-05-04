package com.twitter.heron.integration_test.common.bolt;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.integration_test.core.BaseBatchBolt;


/**
 * The bolt will maintain a map. It receives word and count the frequency inside the map.
 * It will emit the map when all words are received.
 */

public class PartialUniquerBolt<T> extends BaseBatchBolt<T> {
  private static final long serialVersionUID = 6662874569586264236L;
  OutputCollector collector;
  HashMap<String, Integer> cache = new HashMap<>();

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    // Insert it into the cache
    String lang = tuple.getString(0);
    Integer count = cache.get(lang);
    if (count == null) {
      count = 1;
    } else {
      count++;
    }
    cache.put(lang, count);
  }

  @Override
  public void finishBatch() {
    collector.emit(new Values(cache));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("partial-count"));
  }
}