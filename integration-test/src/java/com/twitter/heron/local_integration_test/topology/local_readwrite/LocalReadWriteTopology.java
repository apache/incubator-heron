package com.twitter.heron.local_integration_test.topology.local_readwrite;

import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;

import com.twitter.heron.integration_test.common.BasicConfig;
import com.twitter.heron.integration_test.common.bolt.IdentityBolt;
import com.twitter.heron.integration_test.common.spout.PausedLocalFileSpout;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;

// This topology is designed use PausedLocalFileSpout to wait until a testing.txt file exists
// Then, the spout will read each line from the file and emit it to the LocalWriteBolt
// LocalWriteBolt will write each line to a separate file, testing2.txt
public final class LocalReadWriteTopology {
  private static final String LOCAL_AGGREGATOR_BOLT_CLASS = "com.twitter.heron.integration_test.core.LocalAggregatorBolt";

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      throw new RuntimeException("Expects 1 or 2 arguments, topology name and max emit count (optional)");
    }
    String topologyName = args[0];
    TestTopologyBuilder builder = new TestTopologyBuilder("testing2.txt");
    builder.setTerminalBoltClass(LOCAL_AGGREGATOR_BOLT_CLASS);
    if (args.length == 1) {
      builder.setSpout("paused-local-spout", new PausedLocalFileSpout("testing.txt"), 1);
    }
    else {
      int maxEmits = Integer.parseInt(args[1]);
      builder.setSpout("paused-local-spout", new PausedLocalFileSpout("testing.txt"), 1, maxEmits);
    }
      
    builder.setBolt("identity-bolt", new IdentityBolt(new Fields("line")), 1)
      .shuffleGrouping("paused-local-spout");

    Config conf = new BasicConfig();

    HeronSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }
}
