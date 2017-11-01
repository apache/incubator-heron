//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.examples.streamlet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.examples.streamlet.utils.StreamletUtils;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KVStreamlet;
import com.twitter.heron.streamlet.Runner;
import com.twitter.heron.streamlet.WindowConfig;

/**
 * This topology demonstrates the use of join operations in the Heron
 * Streamlet API for Java. Two independent streamlets, one consisting
 * of ad impressions, the other of ad clicks, is joined together. A join
 * function then checks if the userId matches on the impression and click.
 * Finally, a reduce function counts the number of impression/click matches
 * over the specified time window.
 */
public final class ImpressionsAndClicksTopology {
  private ImpressionsAndClicksTopology() { 
  }

  private static final Logger LOG =
      Logger.getLogger(ImpressionsAndClicksTopology.class.getName());

  /**
   * A list of company IDs to be used to generate random clicks and impressions.
   */
  private static final List<String> ADS = Arrays.asList(
      "acme",
      "blockchain-inc",
      "omnicorp"
  );

  /**
   * A list of 100 active users ("user1" through "user100").
   */
  private static final List<String> USERS = IntStream.range(1, 100)
      .mapToObj(i -> String.format("user%d", i))
      .collect(Collectors.toList());

  /**
   * A POJO for incoming ad impressions (generated every 50 milliseconds). 
   */
  private static class AdImpression implements Serializable {
    private static final long serialVersionUID = 3283110635310800177L;

    private String adId;
    private String userId;
    private String impressionId;

    AdImpression() {
      Utils.sleep(50);
      this.adId = StreamletUtils.randomFromList(ADS);
      this.userId = StreamletUtils.randomFromList(USERS);
      this.impressionId = UUID.randomUUID().toString();
      LOG.info(String.format("Emitting impression: %s", this));
    }

    String getAdId() {
      return adId;
    }

    String getUserId() {
      return userId;
    }

    @Override
    public String toString() {
      return String.format("(adId; %s, impressionId: %s)",
          adId,
          impressionId
      );
    }
  }

  /**
   * A POJO for incoming ad clicks (generated every 50 milliseconds).
   */
  private static class AdClick implements Serializable {
    private static final long serialVersionUID = 7202766159176178988L;
    private String adId;
    private String userId;
    private String clickId;

    AdClick() {
      Utils.sleep(50);
      this.adId = StreamletUtils.randomFromList(ADS);
      this.userId = StreamletUtils.randomFromList(USERS);
      this.clickId = UUID.randomUUID().toString();
      LOG.info(String.format("Emitting click: %s", this));
    }

    String getAdId() {
      return adId;
    }

    String getUserId() {
      return userId;
    }

    @Override
    public String toString() {
      return String.format("(adId; %s, clickId: %s)",
          adId,
          clickId
      );
    }
  }

  /**
   * The join function for the processing graph. For each incoming KeyValue object
   * pair joined during the specified time window, if the userId matches then a KeyValue
   * object with a value of 1 is returned (else 0).
   */
  private static KeyValue<String, Integer> incrementIfSameUser(String userId1, String userId2) {
    return (userId1.equals(userId2)) ? new KeyValue<>(userId1, 1) : new KeyValue<>(userId1, 0);
  }

  /**
   * The reduce function for the processing graph. A cumulative total of clicks is counted
   * for each userId.
   */
  private static KeyValue<String, Integer> countCumulativeClicks(
      KeyValue<String, Integer> cumulative,
      KeyValue<String, Integer> incoming) {
    int total = cumulative.getValue() + incoming.getValue();
    return new KeyValue<>(incoming.getKey(), total);
  }

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.createBuilder();

    /**
     * A KVStreamlet is produced. Each element is a KeyValue object where the key
     * is the impression ID and the user ID is the value.
     */
    KVStreamlet<String, String> impressions = processingGraphBuilder
        .newSource(AdImpression::new)
        .mapToKV(impression -> new KeyValue<>(impression.getAdId(), impression.getUserId()));

    /**
     * A KVStreamlet is produced. Each element is a KeyValue object where the key
     * is the ad ID and the user ID is the value.
     */
    KVStreamlet<String, String> clicks = processingGraphBuilder
        .newSource(AdClick::new)
        .mapToKV(click -> new KeyValue<>(click.getAdId(), click.getUserId()));

    /**
     * Here, the impressions KVStreamlet is joined to the clicks KVStreamlet.
     */
    impressions
        /**
         * The join function here essentially provides the reduce function
         * with a streamlet of KeyValue objects where the userId matches across
         * an impression and a click (meaning that the user has clicked on the
         * ad).
         */
        .join(
            clicks,
            WindowConfig.TumblingCountWindow(100),
            ImpressionsAndClicksTopology::incrementIfSameUser
        )
        /**
         * The reduce function counts the number of ad clicks per user.
         */
        .reduceByKeyAndWindow(
            WindowConfig.TumblingCountWindow(200),
            ImpressionsAndClicksTopology::countCumulativeClicks
        )
        .consume(kw -> {
          String userId = kw.getValue().getKey();
          int totalUserClicks = kw.getValue().getValue();

          LOG.info(String.format("(user: %s, clicks: %d)", userId, totalUserClicks));
        });

    Config config = new Config();

    /**
     * Fetches the topology name from the first command-line argument
     */
    String topologyName = StreamletUtils.getTopologyName(args);

    /**
     * Finally, the processing graph and configuration are passed to the Runner,
     * which converts the graph into a Heron topology that can be run in a Heron
     * cluster.
     */
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
