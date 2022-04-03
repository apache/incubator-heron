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

package org.apache.heron.eco;

import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.api.Config;
import org.apache.heron.api.utils.Slf4jUtils;
import org.apache.heron.eco.builder.BoltBuilder;
import org.apache.heron.eco.builder.BuilderUtility;
import org.apache.heron.eco.builder.ComponentBuilder;
import org.apache.heron.eco.builder.ConfigBuilder;
import org.apache.heron.eco.builder.ObjectBuilder;
import org.apache.heron.eco.definition.BoltDefinition;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.definition.SpoutDefinition;
import org.apache.heron.eco.definition.StreamDefinition;
import org.apache.heron.eco.parser.EcoParser;
import org.apache.heron.eco.submit.EcoSubmitter;

public class Eco {

  private static final Logger LOG = Logger.getLogger(Eco.class.getName());
  private static final String PROPS = "props";
  private static final String ENV_PROPS = "env-props";
  private static final String ECO_CONFIG_FILE = "eco-config-file";

  private EcoParser ecoParser;
  private EcoSubmitter ecoSubmitter;

  public Eco(EcoParser ecoParser, EcoSubmitter ecoSubmitter) {
    this.ecoParser = ecoParser;
    this.ecoSubmitter = ecoSubmitter;
  }

  /**
   * Submit an ECO topology
   *
   * @param fileInputStream  The input stream associated with ECO topology definition file
   * @param propertiesFile  The optional key-value property file for optional property substitution.
   * @param envFilter The optional flag to tell ECO to perform environment variable substitution
   * @throws Exception the exception thrown
   */
  public void submit(FileInputStream fileInputStream,
                     FileInputStream propertiesFile, boolean envFilter)
      throws Exception {
    EcoTopologyDefinition topologyDefinition = ecoParser
        .parseFromInputStream(fileInputStream, propertiesFile, envFilter);

    String topologyName = topologyDefinition.getName();
    String topologyType = topologyDefinition.getType();

    if ("storm".equals(topologyType)) {
      System.out.println("topology type is Storm");
      org.apache.heron.eco.builder.storm.EcoBuilder ecoBuilder =
          new org.apache.heron.eco.builder.storm.EcoBuilder(
            new org.apache.heron.eco.builder.storm.SpoutBuilder(),
            new BoltBuilder(),
            new org.apache.heron.eco.builder.storm.StreamBuilder(),
            new ComponentBuilder(),
            new ConfigBuilder());

      Config topologyConfig = ecoBuilder
          .buildConfig(topologyDefinition);

      EcoExecutionContext executionContext
          = new EcoExecutionContext(topologyDefinition, topologyConfig);

      printTopologyInfo(executionContext);

      ObjectBuilder objectBuilder = new ObjectBuilder();
      objectBuilder.setBuilderUtility(new BuilderUtility());

      org.apache.storm.topology.TopologyBuilder builder = ecoBuilder
          .buildTopologyBuilder(executionContext, objectBuilder);
      ecoSubmitter.submitStormTopology(topologyName, topologyConfig, builder.createTopology());
    } else if ("heron".equals(topologyType)) {
      System.out.println("topology type is Heron");
      org.apache.heron.eco.builder.heron.EcoBuilder ecoBuilder =
          new org.apache.heron.eco.builder.heron.EcoBuilder(
            new org.apache.heron.eco.builder.heron.SpoutBuilder(),
            new BoltBuilder(),
            new org.apache.heron.eco.builder.heron.StreamBuilder(),
            new ComponentBuilder(),
            new ConfigBuilder());

      Config topologyConfig = ecoBuilder
          .buildConfig(topologyDefinition);

      EcoExecutionContext executionContext
          = new EcoExecutionContext(topologyDefinition, topologyConfig);

      printTopologyInfo(executionContext);

      ObjectBuilder objectBuilder = new ObjectBuilder();
      objectBuilder.setBuilderUtility(new BuilderUtility());

      org.apache.heron.api.topology.TopologyBuilder builder = ecoBuilder
          .buildTopologyBuilder(executionContext, objectBuilder);
      ecoSubmitter.submitHeronTopology(topologyName, topologyConfig, builder.createTopology());
    } else {
      LOG.log(Level.SEVERE,
          String.format("Unknown topology type \'%s\' for topology %s, not submitted",
              topologyType, topologyName));
    }
  }

  public static void main(String[] args) throws Exception {
    Slf4jUtils.installSLF4JBridge();
    Options options = constructOptions();

    CommandLineParser parser = new DefaultParser();

    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing command line options: ", e);
    }


    FileInputStream fin = new FileInputStream(new File(cmd.getOptionValue(ECO_CONFIG_FILE)));

    String propsFile = cmd.getOptionValue(PROPS);
    FileInputStream propsInputStream = null;

    if (propsFile != null) {
      propsInputStream = new FileInputStream(new File(propsFile));
    }


    Boolean filterFromEnv = cmd.hasOption(ENV_PROPS);

    Eco eco = new Eco(new EcoParser(), new EcoSubmitter());

    eco.submit(fin, propsInputStream, filterFromEnv);
  }

  private static Options constructOptions() {
    Options options = new Options();
    Option ecoConfig = Option.builder("eco")
        .desc("Yaml config file for specifying topology definitions")
        .longOpt(ECO_CONFIG_FILE)
        .hasArgs()
        .argName(ECO_CONFIG_FILE)
        .required()
        .build();


    Option filterOption = Option.builder(PROPS)
        .desc("properties file for property substitution")
        .longOpt(PROPS)
        .hasArgs()
        .required(false)
        .argName(PROPS)
        .build();

    Option envSubOption = Option.builder("envFilter")
        .desc("Perform environment variable substitution.")
        .longOpt(ENV_PROPS)
        .numberOfArgs(0)
        .required(false)
        .build();

    options.addOption(filterOption);
    options.addOption(ecoConfig);
    options.addOption(envSubOption);

    return options;
  }

  // construct command line help options
  //TODO: (joshfischer) integrate with existing system somehow
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
