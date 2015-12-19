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


/**
 * A bolt that read a tweet in json format, and then filter out the tweets, whose
 * the top-level property given is equal to the value given, and emit it.
 * Example, given the property "lang" and value "zh", this bolt will read a tweet and emit the tweet
 * , whose "lang" is "zh"
 */

public class JSONTweetFilterBolt extends BaseBasicBolt {
  private static final Logger LOG = Logger.getLogger(JSONTweetFilterBolt.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();

  String propName;
  Object propValue;

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
      tweetJson = mapper.readValue(tweet, Map.class);
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

