//  Copyright 2018 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.examples.eco;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings({"serial", "rawtypes", "unchecked"})
public class EvenAndOddBolt extends BaseRichBolt {

  private Map<String, Integer> namedSpecificIntegers;

  private OutputCollector collector;

  protected Map<String, Integer> getTupleValue(Tuple t, int idx) {

    return (Map<String, Integer>) t.getValues().get(idx);
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    namedSpecificIntegers = new HashMap<>();
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    Map<String, Integer> integerMap = getTupleValue(input, 0);
    for (Map.Entry entry: integerMap.entrySet()) {
      if ((Integer) entry.getValue() % 2 == 0) {
        this.collector.ack(input);
      } else {
        this.collector.fail(input);
      }
    }

  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}
