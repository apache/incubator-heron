package com.twitter.heron.integration_test.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

/**
 * A Bolt which collects the tuples, converts them into json,
 * and posts the json into the given http server.
 */
public class AggregatorBolt extends BaseBatchBolt implements ITerminalBolt {
  private static final Logger LOG = Logger.getLogger(AggregatorBolt.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();

  private final String httpPostUrl;

  private final List<String> result;

  public AggregatorBolt(String httpPostUrl) {
    LOG.info("HttpPostUrl : " + httpPostUrl);
    this.httpPostUrl = httpPostUrl;
    this.result = new ArrayList<String>();
  }

  @Override
  public void finishBatch() {
    // Convert to String and emit it
    writeFinishedData();
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
  }

  @Override
  public void execute(Tuple tuple) {
    // Once we get something, convert to JSON String
    String tupleInJSON = "";
    try {
      tupleInJSON = mapper.writeValueAsString(tuple.getValue(0));
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

  private int postResultToHttpServer(String httpPostUrl, String resultJson) throws Exception {
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(httpPostUrl);

    StringEntity requestEntity = new StringEntity(
        resultJson,
        "application/json",
        "UTF-8"
    );

    post.setEntity(requestEntity);
    HttpResponse response = client.execute(post);

    int responseCode = response.getStatusLine().getStatusCode();

    if (responseCode == 200) {
      LOG.info("Http post successful");
    } else {
      LOG.severe(String.format("Http post failed, response code: %d, response: %s",
              responseCode,
              EntityUtils.toString(response.getEntity()))
      );
    }

    return responseCode;
  }

  public void writeFinishedData() {
    String resultJson = result.toString();
    LOG.info("Actual result: " + resultJson);
    LOG.info("Posting actual result to " + httpPostUrl);
    try {
      int responseCode = postResultToHttpServer(httpPostUrl, resultJson);
      if (responseCode != 200) {
        responseCode = postResultToHttpServer(httpPostUrl, resultJson);
        if (responseCode != 200) {
          throw new RuntimeException(" ResponseCode " + responseCode);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Posting result to server failed with : " + e.getMessage(), e);
    }
  }
}
