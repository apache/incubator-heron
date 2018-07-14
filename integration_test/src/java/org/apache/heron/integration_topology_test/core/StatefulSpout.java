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

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;

public class StatefulSpout extends BaseRichSpout implements IStatefulComponent<String, Integer> {

  private static final long serialVersionUID = 2045875254384424423L;
  protected SpoutOutputCollector collector;
  protected State<String, Integer> state;
  protected TopologyContext context;

  public StatefulSpout() { }

  @Override
  public void open(Map<String, Object> conf,
                   TopologyContext newContext,
                   SpoutOutputCollector newCollector) {
    this.context = newContext;
    this.collector = newCollector;
  }

  @Override
  public void nextTuple() { }

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
