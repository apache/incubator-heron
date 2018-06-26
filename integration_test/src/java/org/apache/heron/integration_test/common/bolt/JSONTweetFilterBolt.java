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
 * A bolt that read a tweet in json format, and then filter out the tweets, whose
 * the top-level property given is equal to the value given, and emit it.
 * Example, given the property "lang" and value "zh", this bolt will read a tweet and emit the tweet
 * , whose "lang" is "zh"
 */

public class JSONTweetFilterBolt extends BaseBasicBolt {
  private static final long serialVersionUID = -7159267336583563993L;
  private static final Logger LOG = Logger.getLogger(JSONTweetFilterBolt.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String propName;
  private Object propValue;

  public JSONTweetFilterBolt(String propName, Object propValue) {
    this.propName = propName;
    this.propValue = propValue;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String tweet = tuple.getString(0);

    if (isFiltered(tweet)) {
      basicOutputCollector.emit(new Values(tweet));

    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(propName));
  }

  /**
   * Judge whether filter a tweet or not
   */
  private boolean isFiltered(String tweet) {
    // Parse JSON entry
    Map<String, Object> tweetJson = null;
    try {
      tweetJson = MAPPER.readValue(tweet, new TypeReference<Map<String, Object>>() { });
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to parse the String into map: " + tweet, e);
    }

    Object propVal = null;
    if (tweetJson == null) {
      propVal = "";
    } else {
      // Is this a tweet
      if (tweetJson.containsKey("id")
          && tweetJson.containsKey("created_at")
          && tweetJson.containsKey("text")
          && tweetJson.containsKey("user")) {


        propVal = tweetJson.get(this.propName);
        if (propVal.equals(this.propValue)) {
          return true;
        }
      }
    }

    return false;
  }
}

