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

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;

public class StatefulIntegrationTopologyTestBolt extends StatefulBolt {

  private static final long serialVersionUID = 6657584414733837761L;
  private static final Logger LOG =
      Logger.getLogger(StatefulIntegrationTopologyTestBolt.class.getName());
  private final StatefulBolt delegateBolt;
  private boolean checkpointExisted = false;
  private String outputLocation;

  public StatefulIntegrationTopologyTestBolt(StatefulBolt delegate, String outputLocation) {
    this.delegateBolt = delegate;
    this.outputLocation = outputLocation;
  }

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext context,
                      OutputCollector outputCollector) {

    this.delegateBolt.prepare(map, context, outputCollector);

    // send instance state to http server
    if (!delegateBolt.state.isEmpty()) {
      String compId = String.format("%s_%d", delegateBolt.context.getThisComponentId(),
          delegateBolt.context.getThisTaskId());
      String dataName = String.format("instance %s state", compId);
      String stateJsonString = formatJson(compId, delegateBolt.state);

      LOG.info(String.format("Posting %s to %s", dataName, this.outputLocation));
      HttpUtils.postToHttpServer(this.outputLocation, stateJsonString, dataName);
    }
  }

  @Override
  public void execute(Tuple tuple) {
    String streamID = tuple.getSourceStreamId();
    LOG.info("Received a tuple: " + tuple + " ; from: " + streamID);
    delegateBolt.execute(tuple);
  }

  @Override
  public void cleanup() {
    delegateBolt.cleanup();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    delegateBolt.declareOutputFields(outputFieldsDeclarer);
  }

  @Override
  public void initState(State<String, Integer> state) {
    delegateBolt.initState(state);
  }

  @Override
  public void preSave(String checkpointId) {
    if (!checkpointExisted) {
      delegateBolt.preSave(checkpointId);
      checkpointExisted = true;
    } else {
      if (delegateBolt.context.getThisTaskIndex() == 0) {
        throw new RuntimeException("Kill instance " + context.getThisComponentId());
      }
    }
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
