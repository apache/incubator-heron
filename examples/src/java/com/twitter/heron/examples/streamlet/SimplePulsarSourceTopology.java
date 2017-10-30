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

import com.twitter.heron.examples.streamlet.utils.StreamletUtils;
import com.twitter.heron.streamlet.*;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.UnsupportedEncodingException;

public class SimplePulsarSourceTopology {
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

        public void setup(Context context) {
            try {
                client = PulsarClient.create(pulsarConnectionUrl);
                consumer = client.subscribe(consumeTopic, subscription);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }

        public String get() {
            try {
                return new String(consumer.receive().getData(), "utf-8");
            } catch (PulsarClientException | UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        public void cleanup() {}
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        Source<String> pulsarSource = new PulsarSource(
                "pulsar://localhost:6650", // Pulsar connection URL
                "persistent://sample/standalone/ns1/heron-pulsar-test-topic", // Pulsar topic
                "subscription-1" // Subscription name for Pulsar topic
        );

        processingGraphBuilder.newSource(pulsarSource)
                .log();

        Config config = new Config();
        config.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}
