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


package org.apache.heron.examples.streamlet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;

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
   * A list of 25 active users ("user1" through "user25").
   */
  private static final List<String> USERS = IntStream.range(1, 25)
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
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.newBuilder();

    // A KVStreamlet is produced. Each element is a KeyValue object where the key
    // is the impression ID and the user ID is the value.
    Streamlet<AdImpression> impressions = processingGraphBuilder
        .newSource(AdImpression::new);

    // A KVStreamlet is produced. Each element is a KeyValue object where the key
    // is the ad ID and the user ID is the value.
    Streamlet<AdClick> clicks = processingGraphBuilder
        .newSource(AdClick::new);

    /**
     * Here, the impressions KVStreamlet is joined to the clicks KVStreamlet.
     */
    impressions
        // The join function here essentially provides the reduce function with a streamlet
        // of KeyValue objects where the userId matches across an impression and a click
        // (meaning that the user has clicked on the ad).
        .join(
            // The other streamlet that's being joined to
            clicks,
            // Key extractor for the impressions streamlet
            impression -> impression.getUserId(),
            // Key extractor for the clicks streamlet
            click -> click.getUserId(),
            // Window configuration for the join operation
            WindowConfig.TumblingCountWindow(25),
            // Join type (inner join means that all elements from both streams will be included)
            JoinType.INNER,
            // For each element resulting from the join operation, a value of 1 will be provided
            // if the ad IDs match between the elements (or a value of 0 if they don't).
            (user1, user2) -> (user1.getAdId().equals(user2.getAdId())) ? 1 : 0
        )
        // The reduce function counts the number of ad clicks per user.
        .reduceByKeyAndWindow(
            // Key extractor for the reduce operation
            kv -> String.format("user-%s", kv.getKey().getKey()),
            // Value extractor for the reduce operation
            kv -> kv.getValue(),
            // Window configuration for the reduce operation
            WindowConfig.TumblingCountWindow(50),
            // A running cumulative total is calculated for each key
            (cumulative, incoming) -> cumulative + incoming
        )
        // Finally, the consumer operation provides formatted log output
        .consume(kw -> {
          LOG.info(String.format("(user: %s, clicks: %d)",
              kw.getKey().getKey(),
              kw.getValue()));
        });

    Config config = Config.defaultConfig();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
