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
package com.twitter.heron.eco;

import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;

import com.twitter.heron.eco.builder.BoltBuilder;
import com.twitter.heron.eco.builder.ComponentBuilder;
import com.twitter.heron.eco.builder.EcoBuilder;
import com.twitter.heron.eco.builder.SpoutBuilder;
import com.twitter.heron.eco.builder.StreamBuilder;
import com.twitter.heron.eco.definition.BoltDefinition;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.SpoutDefinition;
import com.twitter.heron.eco.definition.StreamDefinition;
import com.twitter.heron.eco.parser.EcoParser;


public final class Eco {

  private static final Logger LOG = Logger.getLogger(Eco.class.getName());

  private Eco() { }

  public static void main(String[] args) throws Exception {
    Options options = constructOptions();

    CommandLineParser parser = new DefaultParser();

    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    FileInputStream fin = new FileInputStream(new File(cmd.getOptionValue("eco-config-file")));

    EcoTopologyDefinition topologyDefinition = EcoParser.parseFromInputStream(fin);

    String topologyName = topologyDefinition.getName();

    EcoBuilder ecoBuilder = createEcoBuilder();

    Config topologyConfig = ecoBuilder
        .buildConfig(topologyDefinition);

    EcoExecutionContext executionContext
        = new EcoExecutionContext(topologyDefinition, topologyConfig);

    printTopologyInfo(executionContext);

    TopologyBuilder builder = ecoBuilder
        .buildTopologyBuilder(executionContext);

    HeronSubmitter.submitTopology(topologyName, topologyConfig, builder.createTopology());

  }

  private static EcoBuilder createEcoBuilder() {
    SpoutBuilder spoutBuilder = new SpoutBuilder();
    BoltBuilder boltBuilder = new BoltBuilder();
    StreamBuilder streamBuilder = new StreamBuilder();
    ComponentBuilder componentBuilder = new ComponentBuilder();

    return new EcoBuilder(spoutBuilder, boltBuilder, streamBuilder, componentBuilder);
  }

  private static Options constructOptions() {
    Options options = new Options();
    Option ecoConfig = Option.builder("eco")
        .desc("Yaml config file for specifying topology definitions")
        .longOpt("eco-config-file")
        .hasArgs()
        .argName("eco-config-file")
        .required()
        .build();
    options.addOption(ecoConfig);
    return options;
  }

  // construct command line help options
  private static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  static void printTopologyInfo(EcoExecutionContext ctx) {
    EcoTopologyDefinition t = ctx.getTopologyDefinition();

    LOG.info("---------- TOPOLOGY DETAILS ----------");

    LOG.info(String.format("Topology Name: %s", t.getName()));
    LOG.info("--------------- SPOUTS ---------------");
    for (SpoutDefinition s : t.getSpouts()) {
      LOG.info(String.format("%s [%d] (%s)", s.getId(), s.getParallelism(), s.getClassName()));
    }
    LOG.info("---------------- BOLTS ---------------");
    for (BoltDefinition b : t.getBolts()) {
      LOG.info(String.format("%s [%d] (%s)", b.getId(), b.getParallelism(), b.getClassName()));
    }

    LOG.info("--------------- STREAMS ---------------");
    for (StreamDefinition sd : t.getStreams()) {
      LOG.info(String.format("%s --%s--> %s",
          sd.getFrom(),
          sd.getGrouping().getType(),
          sd.getTo()));
    }
    LOG.info("--------------------------------------");
  }
}
