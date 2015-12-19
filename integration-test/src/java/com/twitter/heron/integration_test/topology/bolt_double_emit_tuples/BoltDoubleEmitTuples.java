package com.twitter.heron.integration_test.topology.bolt_double_emit_tuples;

import java.net.URL;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;
import com.twitter.heron.integration_test.common.bolt.DoubleTuplesBolt;
import com.twitter.heron.integration_test.common.BasicConfig;
import com.twitter.heron.integration_test.common.spout.ABSpout;

/**
 * Topology to test basic structure, single thread spout & bolt, shuffleGrouping
 */
public class BoltDoubleEmitTuples {
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      throw new RuntimeException("HttpServerUrl and TopologyName are needed as command line arguments");
    }

    URL httpServerUrl = new URL(args[0]);
    String topologyName = args[1];

    TestTopologyBuilder builder = new TestTopologyBuilder(topologyName, httpServerUrl.toString());

    builder.setSpout("ab-spout", new ABSpout(), 1);
    builder.setBolt("double-tuples-bolt", new DoubleTuplesBolt(new Fields("word")), 1)
        .shuffleGrouping("ab-spout");

    // Conf
    Config conf = new BasicConfig();

    // Submit it!
    HeronSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }
}
