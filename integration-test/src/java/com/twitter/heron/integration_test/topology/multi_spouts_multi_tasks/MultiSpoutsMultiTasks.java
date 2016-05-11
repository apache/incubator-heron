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
package com.twitter.heron.integration_test.topology.multi_spouts_multi_tasks;

import java.net.URL;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.integration_test.common.BasicConfig;
import com.twitter.heron.integration_test.common.bolt.IdentityBolt;
import com.twitter.heron.integration_test.common.spout.ABSpout;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;

/**
 * Topology to test Spout with multiple threads
 */
public final class MultiSpoutsMultiTasks {

  private MultiSpoutsMultiTasks() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new RuntimeException("HttpServerUrl and TopologyName "
          + "are needed as command line arguments");
    }

    URL httpServerUrl = new URL(args[0]);
    String topologyName = args[1];

    TestTopologyBuilder builder = new TestTopologyBuilder(topologyName, httpServerUrl.toString());

    builder.setSpout("ab-spout-1", new ABSpout(), 3);
    builder.setSpout("ab-spout-2", new ABSpout(), 3);
    builder.setBolt("identity-bolt", new IdentityBolt(new Fields("word")), 1)
        .shuffleGrouping("ab-spout-1")
        .shuffleGrouping("ab-spout-2");

    // Conf
    Config conf = new BasicConfig();

    // Submit it!
    HeronSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }
}
