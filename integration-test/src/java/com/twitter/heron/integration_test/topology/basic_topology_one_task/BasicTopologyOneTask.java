package com.twitter.heron.integration_test.topology.basic_topology_one_task;

import java.net.URL;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;
import com.twitter.heron.integration_test.common.bolt.IdentityBolt;
import com.twitter.heron.integration_test.common.BasicConfig;
import com.twitter.heron.integration_test.common.spout.ABSpout;

/**
 * Topology to test basic structure, single thread spout & bolt, shuffleGrouping
 */
public final class BasicTopologyOneTask {
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      throw new RuntimeException("HttpServerUrl and TopologyName are needed as command line arguments");
    }

    URL httpServerUrl = new URL(args[0]);
    String topologyName = args[1];

    TestTopologyBuilder builder = new TestTopologyBuilder(topologyName, httpServerUrl.toString());

    builder.setSpout("ab-spout", new ABSpout(), 1);
    builder.setBolt("identity-bolt", new IdentityBolt(new Fields("word")), 1)
        .shuffleGrouping("ab-spout");

    // Conf
    Config conf = new BasicConfig();

    // Submit it!
    HeronSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }
}
