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
import java.util.concurrent.ThreadLocalRandom;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

import backtype.storm.utils.Utils;

public class StatefulRandomIntSpout extends BaseRichSpout
    implements IStatefulComponent<String, Integer> {
  private SpoutOutputCollector spoutOutputCollector;
  private State<String, Integer> count;

  public StatefulRandomIntSpout() {
  }

  // Generates a random integer between 1 and 100
  private int randomInt() {
    return ThreadLocalRandom.current().nextInt(1, 101);
  }

  // These two methods are required to implement the IStatefulComponent interface
  @Override
  public void preSave(String checkpointId) {
    System.out.println(String.format("Saving spout state at checkpoint %s", checkpointId));
  }

  @Override
  public void initState(State<String, Integer> state) {
    count = state;
  }

  // These three methods are required to extend the BaseRichSpout abstract class
  @Override
  public void open(Map<String, Object> map, TopologyContext ctx, SpoutOutputCollector collector) {
    spoutOutputCollector = collector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("random-int"));
  }

  @Override
  public void nextTuple() {
    Utils.sleep(2000);
    int randomInt = randomInt();
    System.out.println("Emitting Value: " + randomInt);
    spoutOutputCollector.emit(new Values(randomInt));
  }
}

