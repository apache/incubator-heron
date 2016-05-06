// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
 * The bolt receives map, and put all entries in received map into a buffer map,
 * and emit this buffer map when it receives all the maps (in finishBatch()).
 */

public class MapAggregatorBolt extends BaseBatchBolt {
  private static final long serialVersionUID = -3500154155463300293L;
  private OutputCollector collector;
  private HashMap<String, Integer> buffer = new HashMap<>();

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void execute(Tuple tuple) {
    buffer.putAll((Map<String, Integer>) tuple.getValue(0));
  }

  @Override
  public void finishBatch() {
    collector.emit(new Values(buffer));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("count-res"));
  }
}


