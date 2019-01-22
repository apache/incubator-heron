/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.examples.eco;


import java.util.Map;

import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.bolt.IBasicBolt;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;

import static org.apache.heron.api.utils.Utils.tuple;

@SuppressWarnings({"serial", "rawtypes", "unchecked"})
public class EvenAndOddBolt implements IBasicBolt {


  @Override
  public void prepare(Map stormConf, TopologyContext context) {

  }

  protected int getTupleValue(Tuple t, int idx) {
    return (int) t.getValues().get(idx);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    int number = getTupleValue(input, 0);

    if (number % 2 == 0) {
      System.out.println("Emitting to evens stream: " + number);
      collector.emit("evens", tuple(input.getValues().get(0)));

    } else {
      System.out.println("emitting to odds stream: " + number);
      collector.emit("odds", tuple(input.getValues().get(0)));
    }

    collector.emit(tuple(input.getValues().get(0)));



  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("evens", new Fields("evens"));
    declarer.declareStream("odds", new Fields("odds"));
    declarer.declare(new Fields("number"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
