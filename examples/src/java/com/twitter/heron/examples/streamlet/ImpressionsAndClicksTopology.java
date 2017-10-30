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

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.*;
import io.streaml.heron.streamlet.utils.StreamletUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ImpressionsAndClicksTopology {
    private static final Logger LOG = Logger.getLogger(ImpressionsAndClicksTopology.class.getName());

    private static final List<String> ADS = Arrays.asList(
            "acme",
            "blockchain-inc",
            "omnicorp"
    );

    private static final List<String> USERS = IntStream.range(1, 100)
            .mapToObj(i -> String.format("user%d", i))
            .collect(Collectors.toList());

    private static class AdImpression implements Serializable {
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
            return String.format(
                    "(adId; %s, impressionId: %s)",
                    adId,
                    impressionId
            );
        }
    }

    private static class AdClick implements Serializable {
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
            return String.format(
                    "(adId; %s, clickId: %s)",
                    adId,
                    clickId
            );
        }
    }

    private static int incrementIfSameUser(String userId1, String userId2) {
        return (userId1.equals(userId2)) ? 1 : 0;
    }

    private static int countCumulativeClicks(int cumulative, int incoming) {
        return cumulative + incoming;
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        KVStreamlet<String, String> impressions = processingGraphBuilder.newSource((AdImpression::new))
                .mapToKV(impression -> new KeyValue<>(impression.getAdId(), impression.getUserId()));
        KVStreamlet<String, String> clicks = processingGraphBuilder.newSource(AdClick::new)
                .mapToKV(click -> new KeyValue<>(click.getAdId(), click.getUserId()));

        impressions
                .join(clicks, WindowConfig.TumblingCountWindow(100), ImpressionsAndClicksTopology::incrementIfSameUser)
                .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(200), ImpressionsAndClicksTopology::countCumulativeClicks)
                .log();

        Config config = new Config();

        String topologyName = StreamletUtils.getTopologyName(args);

        // Finally, convert the processing graph and configuration into a Heron topology
        // and run it in a Heron cluster.
        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}
