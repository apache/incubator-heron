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

package org.apache.heron.integration_topology_test.common;

import java.net.MalformedURLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.integration_topology_test.core.TopologyTestTopologyBuilder;

/**
 * Class to abstract out the common parts of the test framework for submitting topologies.
 * Subclasses can implement {@code buildTopology} and call {@code submit}.
 */
public abstract class AbstractTestTopology {
  private static final Integer CHECKPOINT_INTERVAL = 10;
  private static final String TOPOLOGY_OPTION = "topology_name";
  private static final String RESULTS_URL_OPTION = "results_url";

  private final CommandLine cmd;
  private final String topologyName;
  private final String httpServerResultsUrl;

  protected AbstractTestTopology(String[] args) throws MalformedURLException {
    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    Options options = getArgOptions();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      formatter.setWidth(120);
      formatter.printHelp("java " + getClass().getCanonicalName(), options, true);
      throw new RuntimeException(e);
    }

    this.topologyName = cmd.getOptionValue(TOPOLOGY_OPTION);
    if (cmd.getOptionValue(RESULTS_URL_OPTION) != null) {
      this.httpServerResultsUrl =
          pathAppend(cmd.getOptionValue(RESULTS_URL_OPTION), this.topologyName);
    } else {
      this.httpServerResultsUrl = null;
    }
  }

  protected TopologyBuilder buildTopology(TopologyBuilder builder) {
    return builder;
  }
  protected TopologyTestTopologyBuilder buildStatefulTopology(TopologyTestTopologyBuilder
                                                                           builder) {
    return builder;
  }

  protected Config buildConfig(Config config) {
    return config;
  }

  protected Options getArgOptions() {
    Options options = new Options();

    Option topologyNameOption = new Option("t", TOPOLOGY_OPTION, true, "topology name");
    topologyNameOption.setRequired(true);
    options.addOption(topologyNameOption);

    Option resultsUrlOption =
        new Option("r", RESULTS_URL_OPTION, true, "url to post and get instance state");
    resultsUrlOption.setRequired(false);
    options.addOption(resultsUrlOption);

    return options;
  }

  public final void submit() throws AlreadyAliveException, InvalidTopologyException {
    this.submit(null);
  }

  public final void submit(Config userConf) throws AlreadyAliveException, InvalidTopologyException {
    Config conf = buildConfig(new BasicConfig());
    if (userConf != null) {
      conf.putAll(userConf);
    }

    if (this.httpServerResultsUrl == null) {
      TopologyBuilder builder = new TopologyBuilder();
      HeronSubmitter.submitTopology(topologyName, conf, buildTopology(builder).createTopology());

    } else {
      TopologyTestTopologyBuilder builder = new TopologyTestTopologyBuilder(httpServerResultsUrl);
      conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
      conf.setTopologyStatefulCheckpointIntervalSecs(CHECKPOINT_INTERVAL);
      HeronSubmitter.submitTopology(topologyName, conf,
          buildStatefulTopology(builder).createTopology());
    }
  }

  private static String pathAppend(String url, String path) {
    return String.format("%s/%s", url, path);
  }
}
