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
package com.twitter.heron.local_integration_test.topology.local_readwrite;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.integration_test.common.BasicConfig;
import com.twitter.heron.integration_test.common.bolt.IdentityBolt;
import com.twitter.heron.integration_test.common.spout.PausedLocalFileSpout;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;

// This topology is designed use PausedLocalFileSpout to wait until a testing.txt file exists
// Then, the spout will read each line from the file and emit it to the LocalWriteBolt
// LocalWriteBolt will write each line to a separate file, testing2.txt
public final class LocalReadWriteTopology {
  private static final String LOCAL_AGGREGATOR_BOLT_CLASS =
      "com.twitter.heron.integration_test.core.LocalAggregatorBolt";

  private LocalReadWriteTopology() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 4) {
      throw new RuntimeException("Expects 3 or 4 arguments, topology name, "
          + "inputFile, outputFile and max emit count (optional)");
    }
    String topologyName = args[0];
    String inputFile = args[1];
    String outputFile = args[2];
    TestTopologyBuilder builder = new TestTopologyBuilder(outputFile);
    builder.setTerminalBoltClass(LOCAL_AGGREGATOR_BOLT_CLASS);
    if (args.length == 3) {
      builder.setSpout("paused-local-spout", new PausedLocalFileSpout(inputFile), 1);
    } else {
      int maxEmits = Integer.parseInt(args[3]);
      builder.setSpout("paused-local-spout", new PausedLocalFileSpout(inputFile), 1, maxEmits);
    }

    builder.setBolt("identity-bolt", new IdentityBolt(new Fields("line")), 1)
        .shuffleGrouping("paused-local-spout");

    Config conf = new BasicConfig();

    HeronSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }
}
