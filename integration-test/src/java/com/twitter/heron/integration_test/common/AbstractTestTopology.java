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
package com.twitter.heron.integration_test.common;

import java.net.MalformedURLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.integration_test.core.TestTopologyBuilder;

/**
 * Class to abstract out the common parts of the test framework for submitting topologies.
 * Subclasses can implement {@code buildTopology} and call {@code submit}.
 */
public abstract class AbstractTestTopology {
  private static final String TOPOLOGY_OPTION = "topology_name";
  private static final String RESULTS_URL_OPTION = "results_url";
  private static final String STATE_URL_OPTION = "test_state_url";
  private static final String STATE_UPDATE_TOKEN = "state_server_update_token";

  private final CommandLine cmd;
  private final String httpServerResultsUrl;
  private final String httpServerStateUrl;
  private final String topologyName;
  private final String stateUpdateToken;

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
    this.httpServerResultsUrl =
        pathAppend(cmd.getOptionValue(RESULTS_URL_OPTION), this.topologyName);
    this.httpServerStateUrl =
        pathAppend(cmd.getOptionValue(STATE_URL_OPTION), this.topologyName);
    this.stateUpdateToken = cmd.getOptionValue(STATE_UPDATE_TOKEN);
  }

  protected abstract TestTopologyBuilder buildTopology(TestTopologyBuilder builder);

  protected Config buildConfig(Config config) {
    return config;
  }

  protected CommandLine getCommandLine() {
    return this.cmd;
  }

  protected Options getArgOptions() {
    Options options = new Options();

    Option topologyNameOption = new Option("t", TOPOLOGY_OPTION, true, "topology name");
    topologyNameOption.setRequired(true);
    options.addOption(topologyNameOption);

    Option resultsUrlOption =
        new Option("r", RESULTS_URL_OPTION, true, "url to post and get test data");
    resultsUrlOption.setRequired(true);
    options.addOption(resultsUrlOption);

    Option stateUrlOption =
        new Option("s", STATE_URL_OPTION, true, "url to post and get test state info");
    stateUrlOption.setRequired(true);
    options.addOption(stateUrlOption);

    Option stateTokenOption = new Option("u", STATE_UPDATE_TOKEN, true,
        "state server token to use for multi-phase test");
    stateTokenOption.setRequired(false);
    options.addOption(stateTokenOption);

    return options;
  }

  public final void submit() throws AlreadyAliveException, InvalidTopologyException {
    TestTopologyBuilder builder =
        new TestTopologyBuilder(httpServerResultsUrl, httpServerStateUrl, stateUpdateToken);

    Config conf = buildConfig(new BasicConfig());
    HeronSubmitter.submitTopology(topologyName, conf, buildTopology(builder).createTopology());
  }

  private static String pathAppend(String url, String path) {
    return String.format("%s/%s", url, path);
  }
}
