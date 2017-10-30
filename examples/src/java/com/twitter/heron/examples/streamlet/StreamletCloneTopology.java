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

import com.twitter.heron.streamlet.*;
import com.twitter.heron.examples.streamlet.utils.StreamletUtils;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamletCloneTopology {
    private static final Logger LOG =
            Logger.getLogger(StreamletCloneTopology.class.getName());

    private static final List<String> PLAYERS = IntStream.range(1, 100)
            .mapToObj(i -> String.format("player%d", i))
            .collect(Collectors.toList());

    private static class GameScore implements Serializable {
        private static final long serialVersionUID = 1089454399729015529L;
        private String userId;
        private int score;

        GameScore() {
            this.userId = StreamletUtils.randomFromList(PLAYERS);
            this.score = ThreadLocalRandom.current().nextInt(1000);
        }

        String getUserId() {
            return userId;
        }

        int getScore() {
            return score;
        }
    }

    private static class StateSink implements Sink<GameScore> {
        private static final long serialVersionUID = 5544736723673011054L;
        private Context ctx;

        public void setup(Context context) {
            this.ctx = context;
        }

        public void put(GameScore score) {
            String key = String.format("score-%s", score.getUserId());
            int incomingScore = score.getScore();
            int currentScore = (int) ctx.getState().getOrDefault(key, 0);
            int cumulativeScore = incomingScore + currentScore;
            cumulativeScore = (cumulativeScore > 10000) ? 0 : cumulativeScore;
            ctx.getState().put(key, cumulativeScore);
        }

        public void cleanup() {}
    }

    private static class FormattedLogSink implements Sink<GameScore> {
        private static final long serialVersionUID = 1251089445039059977L;
        public void setup(Context context) {}

        public void put(GameScore score) {
            String logMessage = String.format("The current score for player %s is %d",
                    score.getUserId(),
                    score.getScore());
            LOG.info(logMessage);
        }

        public void cleanup() {}
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        List<Streamlet<GameScore>> splitGameScoreStreamlet = processingGraphBuilder
                .newSource(GameScore::new)
                .clone(2);

        splitGameScoreStreamlet.get(0)
                .toSink(new StateSink());

        splitGameScoreStreamlet.get(1)
                .toSink(new FormattedLogSink());

        Config config = new Config();

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}