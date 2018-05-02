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

package org.apache.heron.integration_test.common.bolt;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;

/**
 * A bolt that read a tweet in json format, and then filter out the top-level property given and
 * emit it.
 * Example, given the property "id", this bolt will read a tweet and emit the value of key "id" in
 * top level of the tweet's json.
 */
public class TweetPropertyBolt extends BaseBasicBolt {
  private static final long serialVersionUID = -3049021294446207050L;
  private static final Logger LOG = Logger.getLogger(TweetPropertyBolt.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String propName;

  public TweetPropertyBolt(String propName) {
    this.propName = propName;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    Object prop = extract(tuple.getString(0), propName);

    if (prop != null) {
      basicOutputCollector.emit(new Values(prop));

    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(propName));
  }

  /**
   * Extract the prop used from a tweet in json type
   */
  private Object extract(String tweet, String prop) {
    // Parse JSON entry
    Map<String, Object> tweetJson = null;
    try {
      tweetJson = MAPPER.readValue(tweet, new TypeReference<Map<String, Object>>() { });
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to parse the String into map: " + tweet, e);
    }

    Object property = null;
    if (tweetJson == null) {
      property = "";
    } else {
      // Is this a tweet
      if (tweetJson.containsKey("id")
          && tweetJson.containsKey("created_at")
          && tweetJson.containsKey("text")
          && tweetJson.containsKey("user")) {


        property = tweetJson.get(prop);
      }
    }

    return property;
  }
}

