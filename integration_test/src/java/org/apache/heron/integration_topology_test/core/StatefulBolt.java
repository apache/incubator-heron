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
package org.apache.heron.integration_topology_test.core;

import java.util.Map;

import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;

public class StatefulBolt extends BaseRichBolt implements IStatefulComponent<String, Integer> {

  private static final long serialVersionUID = 5834931054885658328L;

  protected State<String, Integer> state;
  protected OutputCollector collector;
  protected TopologyContext context;

  public StatefulBolt() { }

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext inputContext,
                      OutputCollector inputOutputCollector) {
    this.context = inputContext;
    this.collector = inputOutputCollector;
  }

  @Override
  public void execute(Tuple input) { }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) { }

  @Override
  public void initState(State<String, Integer> inputState) {
    this.state = inputState;
  }

  @Override
  public void preSave(String checkpointId) {
  }
}
