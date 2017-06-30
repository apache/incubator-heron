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
package com.twitter.heron.integration_test.topology.one_spout_two_bolts;

import java.net.MalformedURLException;

import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.integration_test.common.AbstractTestTopology;
import com.twitter.heron.integration_test.common.bolt.IdentityBolt;
import com.twitter.heron.integration_test.common.spout.ABSpout;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;

/**
 * Topology to test with one spout connected to two bolts
 */
public final class OneSpoutTwoBolts extends AbstractTestTopology {

  private OneSpoutTwoBolts(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder builder) {
    builder.setSpout("ab-spout", new ABSpout(), 1);
    builder.setBolt("identity-bolt-1", new IdentityBolt(new Fields("word")), 1)
        .shuffleGrouping("ab-spout");
    builder.setBolt("identity-bolt-2", new IdentityBolt(new Fields("word")), 1)
        .shuffleGrouping("ab-spout");
    return builder;
  }

  public static void main(String[] args) throws Exception {
    OneSpoutTwoBolts topology = new OneSpoutTwoBolts(args);
    topology.submit();
  }
}
