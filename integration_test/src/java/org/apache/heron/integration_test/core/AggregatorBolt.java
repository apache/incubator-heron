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

package org.apache.heron.integration_test.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;

/**
 * A Bolt which collects the tuples, converts them into json,
 * and posts the json into the given http server.
 */
public class AggregatorBolt extends BaseBatchBolt implements ITerminalBolt {
  private static final long serialVersionUID = -2994625720418843748L;
  private static final Logger LOG = Logger.getLogger(AggregatorBolt.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String httpPostUrl;

  private final List<String> result;

  public AggregatorBolt(String httpPostUrl) {
    this.httpPostUrl = httpPostUrl;
    this.result = new ArrayList<String>();
  }

  @Override
  public void finishBatch() {
    // Convert to String and emit it
    writeFinishedData();
  }

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
  }

  @Override
  public void execute(Tuple tuple) {
    // Once we get something, convert to JSON String
    String tupleInJSON = "";
    try {
      tupleInJSON = MAPPER.writeValueAsString(tuple.getValue(0));
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE,
          "Could not convert map to JSONString: " + tuple.getValue(0).toString(), e);
    }
    result.add(tupleInJSON);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // The last bolt we append, nothing to emit.
  }

  public void writeFinishedData() {
    String resultJson = result.toString();
    LOG.info(String.format("Posting actual result to %s: %s",  httpPostUrl, resultJson));
    try {
      int responseCode = -1;
      for (int attempts = 0; attempts < 2; attempts++) {
        responseCode = HttpUtils.httpJsonPost(httpPostUrl, resultJson);
        if (responseCode == 200) {
          return;
        }
      }
      throw new RuntimeException(
          String.format("Failed to post actual result to %s: %s",  httpPostUrl, responseCode));
    } catch (IOException | ParseException e) {
      throw new RuntimeException(String.format("Posting result to %s failed",  httpPostUrl), e);
    }
  }
}
