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
import java.net.URL;

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

  private CommandLine cmd;
  private URL httpServerResultsUrl;
  private String topologyName;

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

    this.httpServerResultsUrl = new URL(cmd.getOptionValue(RESULTS_URL_OPTION));
    this.topologyName = cmd.getOptionValue(TOPOLOGY_OPTION);
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

    return options;
  }

  public final void submit() throws AlreadyAliveException, InvalidTopologyException {
    TestTopologyBuilder builder =
        new TestTopologyBuilder(topologyName, httpServerResultsUrl.toString());

    Config conf = buildConfig(new BasicConfig());
    HeronSubmitter.submitTopology(topologyName, conf, buildTopology(builder).createTopology());
  }
}
