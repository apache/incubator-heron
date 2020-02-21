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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;

/**
 * This topology demonstrates clone operations on streamlets in the Heron
 * Streamlet API for Java. A clone operation creates multiple identical copies
 * of a streamlet. Clone operations are especially useful if you want to, for
 * example send streamlet elements to separate sinks, as is done here. A
 * supplier streamlet emits random scores in a game (per player). That initial
 * streamlet is cloned into two. One of the cloned streams goes to a custom
 * logging sink while the other goes to a dummy database sink.
 */
public final class StreamletCloneTopology {
  private StreamletCloneTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(StreamletCloneTopology.class.getName());

  /**
   * A list of players of the game ("player1" through "player100").
   */
  private static final List<String> PLAYERS = IntStream.range(1, 100)
      .mapToObj(i -> String.format("player%d", i))
      .collect(Collectors.toList());

  /**
   * A POJO for game scores.
   */
  private static class GameScore implements Serializable {
    private static final long serialVersionUID = 1089454399729015529L;
    private String playerId;
    private int score;

    GameScore() {
      this.playerId = StreamletUtils.randomFromList(PLAYERS);
      this.score = ThreadLocalRandom.current().nextInt(1000);
    }

    String getPlayerId() {
      return playerId;
    }

    int getScore() {
      return score;
    }
  }

  /**
   * A phony database sink. This sink doesn't actually interact with a database.
   * Instead, it logs each incoming score to stdout.
   */
  private static class DatabaseSink implements Sink<GameScore> {
    private static final long serialVersionUID = 5544736723673011054L;

    private void saveToDatabase(GameScore score) {
      // This is a dummy operation, so no database logic will be implemented here
    }

    public void setup(Context context) {
    }

    public void put(GameScore score) {
      String logMessage = String.format("Saving a score of %d for player %s to the database",
          score.getScore(),
          score.getPlayerId());
      LOG.info(logMessage);
      saveToDatabase(score);
    }

    public void cleanup() {
    }
  }

  /**
   * A logging sink that simply prints a formatted log message for each incoming score.
   */
  private static class FormattedLogSink implements Sink<GameScore> {
    private static final long serialVersionUID = 1251089445039059977L;
    public void setup(Context context) {
    }

    public void put(GameScore score) {
      String logMessage = String.format("The current score for player %s is %d",
          score.getPlayerId(),
          score.getScore());
      LOG.info(logMessage);
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
     * A supplier streamlet of random GameScore objects is cloned into two
     * separate streamlets.
     */
    List<Streamlet<GameScore>> splitGameScoreStreamlet = processingGraphBuilder
        .newSource(GameScore::new)
        .clone(2);

    /**
     * Elements in the first cloned streamlet go to the database sink.
     */
    splitGameScoreStreamlet.get(0)
        .toSink(new DatabaseSink())
        .setName("sink0");

    /**
     * Elements in the second cloned streamlet go to the logging sink.
     */
    splitGameScoreStreamlet.get(1)
        .toSink(new FormattedLogSink())
        .setName("sink1");

    Config config = Config.defaultConfig();

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
