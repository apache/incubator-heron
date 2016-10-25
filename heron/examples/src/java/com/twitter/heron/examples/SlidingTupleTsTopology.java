// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.examples;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * Windowing based on tuple timestamp (e.g. the time when tuple is generated
 * rather than when its processed).
 */
// SUPPRESS CHECKSTYLE HideUtilityClassConstructor
public class SlidingTupleTsTopology {
  public static void main(String[] args) throws Exception {
    Logger.getGlobal().setLevel(Level.FINE);
    TopologyBuilder builder = new TopologyBuilder();
    BaseWindowedBolt bolt = new SlidingWindowSumBolt()
        .withWindow(new Duration(5, TimeUnit.SECONDS), new Duration(3, TimeUnit.SECONDS))
        .withTimestampField("ts")
        .withLag(new Duration(5, TimeUnit.SECONDS));
    builder.setSpout("integer", new RandomIntegerSpout(), 1);
    builder.setBolt("slidingsum", bolt, 1).shuffleGrouping("integer");
    builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("slidingsum");
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(1);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(40000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
