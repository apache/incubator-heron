package com.twitter.heron.integration_test.common.bolt;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

//
//import org.json.simple.JSONObject;
//import org.json.simple.JSONValue;


/**
 * A bolt that read a tweet in json format, and then filter out the top-level property given and
 * emit it.
 * Example, given the property "id", this bolt will read a tweet and emit the value of key "id" in
 * top level of the tweet's json.
 */

public class TweetPropertyBolt extends BaseBasicBolt {
  private static final long serialVersionUID = -3049021294446207050L;
  private static final Logger LOG = Logger.getLogger(TweetPropertyBolt.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();

  String propName;

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
      tweetJson = mapper.readValue(tweet, Map.class);
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

