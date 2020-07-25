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

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.Logger;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Source;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * This topology demonstrates how sources work in the Heron Streamlet API
 * for Java. The example source here reads from an Apache Pulsar topic and
 * injects incoming messages into the processing graph.
 */
public final class SimplePulsarSourceTopology {
  private SimplePulsarSourceTopology() {
  }

  private static final Logger LOG =
          Logger.getLogger(SimplePulsarSourceTopology.class.getName());

  private static class PulsarSource implements Source<String> {
    private static final long serialVersionUID = -3433804102901363106L;
    private PulsarClient client;
    private Consumer consumer;
    private String pulsarConnectionUrl;
    private String consumeTopic;
    private String subscription;

    PulsarSource(String url, String topic, String subscription) {
      this.pulsarConnectionUrl = url;
      this.consumeTopic = topic;
      this.subscription = subscription;
    }

    /**
     * The setup functions defines the instantiation logic for the source.
     * Here, a Pulsar client and consumer are created that will listen on
     * the Pulsar topic.
     */
    public void setup(Context context) {
      try {
        client = PulsarClient.create(pulsarConnectionUrl);
        consumer = client.subscribe(consumeTopic, subscription);
      } catch (PulsarClientException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * The get function defines how elements for the source streamlet are
     * "gotten." In this case, the Pulsar consumer for the specified topic
     * listens for incoming messages.
     */
    public Collection<String> get() {
      try {
        String retval = new String(consumer.receive().getData(), "utf-8");
        return Collections.singletonList(retval);
      } catch (PulsarClientException | UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    public void cleanup() {
    }
  }

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.newBuilder();

    /**
     * A Pulsar source is constructed for a specific Pulsar installation, topic, and
     * subsecription.
     */
    Source<String> pulsarSource = new PulsarSource(
        "pulsar://localhost:6650", // Pulsar connection URL
        "persistent://sample/nomad/ns1/heron-pulsar-test-topic", // Pulsar topic
        "subscription-1" // Subscription name for the Pulsar topic
    );

    /**
     * In this processing graph, the source streamlet consists of messages on a
     * Pulsar topic. Those messages are simply logged without any processing logic
     * applied to them.
     */
    processingGraphBuilder.newSource(pulsarSource)
        .setName("incoming-pulsar-messages")
        .consume(s -> LOG.info(String.format("Message received from Pulsar: \"%s\"", s)));

    Config config = Config.defaultConfig();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
