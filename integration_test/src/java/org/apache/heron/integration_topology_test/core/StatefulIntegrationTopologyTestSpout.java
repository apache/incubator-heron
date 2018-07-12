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
import java.util.logging.Logger;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;

public class StatefulIntegrationTopologyTestSpout extends StatefulSpout {

  private static final long serialVersionUID = -6920782627142720131L;
  private static final Logger LOG = Logger
      .getLogger(StatefulIntegrationTopologyTestSpout.class.getName());

  private final StatefulSpout delegateSpout;
  private int maxExecutions;
  private int curExecutions;
  private String outputLocation;

  public StatefulIntegrationTopologyTestSpout(StatefulSpout delegateSpout,
                                              int maxExecutions, String outputLocation) {
    this.delegateSpout = delegateSpout;
    this.maxExecutions = maxExecutions;
    this.curExecutions = maxExecutions;
    this.outputLocation = outputLocation;
  }

  private void resetMaxExecutions(int resetExecutions) {
    this.curExecutions = resetExecutions;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    delegateSpout.declareOutputFields(outputFieldsDeclarer);
  }

  @Override
  public void close() {
    delegateSpout.close();
  }

  @Override
  public void activate() {
    delegateSpout.activate();
  }

  @Override
  public void deactivate() {
    delegateSpout.deactivate();
  }

  @Override
  public void open(Map<String, Object> map,
                   TopologyContext topologyContext,
                   SpoutOutputCollector outputCollector) {

    delegateSpout.open(map, topologyContext, outputCollector);

    // send instance state to http server
    if (!delegateSpout.state.isEmpty()) {
      String compId = String.format("%s_%d", delegateSpout.context.getThisComponentId(),
          delegateSpout.context.getThisTaskId());
      String dataName = String.format("instance %s state", compId);
      String stateJsonString = formatJson(compId, delegateSpout.state);

      LOG.info(String.format("Posting %s to %s", dataName, this.outputLocation));
      HttpUtils.postToHttpServer(this.outputLocation, stateJsonString, dataName);
    }
  }

  @Override
  public void nextTuple() {
    if (doneEmitting()) {
      return;
    }
    curExecutions--;
    delegateSpout.nextTuple();
  }

  protected boolean doneEmitting() {
    return curExecutions <= 0;
  }

  @Override
  public void initState(State<String, Integer> state) {
    delegateSpout.initState(state);
  }

  @Override
  public void preSave(String checkpointId) {
    delegateSpout.preSave(checkpointId);
    resetMaxExecutions(maxExecutions);
  }

  private String formatJson(String compId, Map<String, Integer> state) {
    StringBuilder stateString = new StringBuilder();
    for (Map.Entry<String, Integer> entry : state.entrySet()) {
      if (stateString.length() != 0) {
        stateString.append(", ");
      }
      stateString.append(String.format("\"%s\": %d", entry.getKey(), entry.getValue()));
    }
    return String.format("{\"%s\": {%s}}", compId, stateString.toString());
  }
}
