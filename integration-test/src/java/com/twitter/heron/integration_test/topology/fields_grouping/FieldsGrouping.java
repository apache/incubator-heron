package com.twitter.heron.integration_test.topology.fields_grouping;

import java.net.URL;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;
import com.twitter.heron.integration_test.common.bolt.CountAggregatorBolt;
import com.twitter.heron.integration_test.common.bolt.WordCountBolt;
import com.twitter.heron.integration_test.common.BasicConfig;
import com.twitter.heron.integration_test.common.spout.ABSpout;

/**
 * Topology to test fields grouping
 */
public class FieldsGrouping {
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      throw new RuntimeException("HttpServerUrl and TopologyName are needed as command line arguments");
    }

    URL httpServerUrl = new URL(args[0]);
    String topologyName = args[1];

    TestTopologyBuilder builder = new TestTopologyBuilder(topologyName, httpServerUrl.toString());

    builder.setSpout("ab-spout", new ABSpout(), 1, 400);
    builder.setBolt("count-bolt", new WordCountBolt(), 2)
        .fieldsGrouping("ab-spout", new Fields("word"));
    builder.setBolt("sum-bolt", new CountAggregatorBolt(), 1)
        .noneGrouping("count-bolt");

    // Conf
    Config conf = new BasicConfig();

    // Submit it!
    HeronSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }

}
