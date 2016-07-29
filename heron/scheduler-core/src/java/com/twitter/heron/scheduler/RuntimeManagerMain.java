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

package com.twitter.heron.scheduler;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;

public class RuntimeManagerMain {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerMain.class.getName());

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("RuntimeManagerMain", options);
  }

  // Construct all required command line options
  private static Options constructOptions() {
    Options options = new Options();

    Option cluster = Option.builder("c")
        .desc("Cluster name in which the topology needs to run on")
        .longOpt("cluster")
        .hasArgs()
        .argName("cluster")
        .required()
        .build();

    Option role = Option.builder("r")
        .desc("Role under which the topology needs to run")
        .longOpt("role")
        .hasArgs()
        .argName("role")
        .required()
        .build();

    Option environment = Option.builder("e")
        .desc("Environment under which the topology needs to run")
        .longOpt("environment")
        .hasArgs()
        .argName("environment")
        .required()
        .build();

    Option topologyName = Option.builder("n")
        .desc("Name of the topology")
        .longOpt("topology_name")
        .hasArgs()
        .argName("topology name")
        .required()
        .build();

    Option heronHome = Option.builder("d")
        .desc("Directory where heron is installed")
        .longOpt("heron_home")
        .hasArgs()
        .argName("heron home dir")
        .required()
        .build();

    Option componentParallelism = Option.builder("a")
        .desc("Component parallelism to update: <name>:<value>,<name>:<value>,...")
        .longOpt("component_parallelism")
        .hasArgs()
        .argName("component parallelism")
        .build();

    Option configFile = Option.builder("p")
        .desc("Path of the config files")
        .longOpt("config_path")
        .hasArgs()
        .argName("config path")
        .required()
        .build();

    Option configOverrides = Option.builder("o")
        .desc("Command line override config path")
        .longOpt("override_config_file")
        .hasArgs()
        .argName("override config file")
        .build();

    Option releaseFile = Option.builder("b")
        .desc("Release file name")
        .longOpt("release_file")
        .hasArgs()
        .argName("release information")
        .build();

    Option command = Option.builder("m")
        .desc("Command to run")
        .longOpt("command")
        .hasArgs()
        .required()
        .argName("command to run")
        .build();

    Option containerId = Option.builder("i")
        .desc("Container Id for restart command")
        .longOpt("container_id")
        .hasArgs()
        .argName("container id")
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(topologyName);
    options.addOption(configFile);
    options.addOption(configOverrides);
    options.addOption(releaseFile);
    options.addOption(command);
    options.addOption(heronHome);
    options.addOption(containerId);
    options.addOption(componentParallelism);
    options.addOption(verbose);

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

  public static void main(String[] args)
      throws ClassNotFoundException, IllegalAccessException,
      InstantiationException, IOException, ParseException {

    Options options = constructOptions();
    Options helpOptions = constructHelpOptions();
    CommandLineParser parser = new DefaultParser();
    // parse the help options first.
    CommandLine cmd = parser.parse(helpOptions, args, true);

    if (cmd.hasOption("h")) {
      usage(options);
      return;
    }

    try {
      // Now parse the required options
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      usage(options);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    Boolean verbose = false;
    Level logLevel = Level.INFO;
    if (cmd.hasOption("v")) {
      logLevel = Level.ALL;
      verbose = true;
    }

    // init log
    LoggingHelper.loggerInit(logLevel, false);

    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    String heronHome = cmd.getOptionValue("heron_home");
    String configPath = cmd.getOptionValue("config_path");
    String overrideConfigFile = cmd.getOptionValue("override_config_file");
    String releaseFile = cmd.getOptionValue("release_file");
    String topologyName = cmd.getOptionValue("topology_name");
    String commandOption = cmd.getOptionValue("command");
    String componentParallelism = cmd.getOptionValue("component_parallelism");

    // Optional argument in the case of restart
    // TODO(karthik): convert into CLI
    String containerId = Integer.toString(-1);
    if (cmd.hasOption("container_id")) {
      containerId = cmd.getOptionValue("container_id");
    }

    Command command = Command.makeCommand(commandOption);

    // first load the defaults, then the config from files to override it
    Config.Builder defaultsConfig = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, configPath, releaseFile));

    // add config parameters from the command line
    Config.Builder commandLineConfig = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.verbose(), verbose)
        .put(Keys.topologyContainerId(), containerId);

    // This is a command line option, but not a valid config key. Hence we don't use Keys
    if (componentParallelism != null) {
      commandLineConfig.put(
          RuntimeManagerRunner.NEW_COMPONENT_PARALLELISM_KEY, componentParallelism);
    }

    Config.Builder topologyConfig = Config.newBuilder()
        .put(Keys.topologyName(), topologyName);

    Config.Builder overrideConfig = Config.newBuilder()
        .putAll(ClusterConfig.loadOverrideConfig(overrideConfigFile));

    // build the final config by expanding all the variables
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultsConfig.build())
            .putAll(overrideConfig.build())
            .putAll(commandLineConfig.build())
            .putAll(topologyConfig.build())
            .build());

    LOG.fine("Static config loaded successfully ");
    LOG.fine(config.toString());

    // Create a new instance of RuntimeManagerMain
    RuntimeManagerMain runtimeManagerMain = new RuntimeManagerMain(config, command);
    boolean isSuccessful = runtimeManagerMain.manageTopology();

    // Log the result and exit
    if (!isSuccessful) {
      throw new RuntimeException(String.format("Failed to %s topology %s", command, topologyName));
    } else {
      LOG.log(Level.FINE, "Topology {0} {1} successfully", new Object[]{topologyName, command});
    }
  }

  // holds all the config read
  private final Config config;

  // command to manage a topology
  private final Command command;

  public RuntimeManagerMain(
      Config config,
      Command command) {
    // initialize the options
    this.config = config;
    this.command = command;
  }

  /**
   * Manager a topology
   * 1. Instantiate necessary resources
   * 2. Valid whether the runtime management is legal
   * 3. Complete the runtime management for a specific command
   *
   * @return true if run runtimeManager successfully
   */
  public boolean manageTopology() {
    String topologyName = Context.topologyName(config);
    // 1. Do prepare work
    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr;
    try {
      statemgr = ReflectionUtils.newInstance(statemgrClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.log(Level.SEVERE, "Failed to instantiate instances", e);
      return false;
    }

    boolean isSuccessful = false;

    // Put it in a try block so that we can always clean resources
    try {
      // initialize the statemgr
      statemgr.initialize(config);

      // TODO(mfu): timeout should read from config
      SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);

      boolean isValid = validateRuntimeManage(adaptor, topologyName);

      // 2. Try to manage topology if valid
      if (isValid) {
        // invoke the appropriate command to manage the topology
        LOG.log(Level.FINE, "Topology: {0} to be {1}ed", new Object[]{topologyName, command});

        // build the runtime config
        Config runtime = Config.newBuilder()
            .put(Keys.topologyName(), Context.topologyName(config))
            .put(Keys.schedulerStateManagerAdaptor(), adaptor)
            .build();

        // Create a ISchedulerClient basing on the config
        ISchedulerClient schedulerClient = getSchedulerClient(runtime);
        if (schedulerClient == null) {
          LOG.severe("Failed to initialize scheduler client");
          return false;
        }

        isSuccessful = callRuntimeManagerRunner(runtime, schedulerClient);
      }
    } finally {
      // 3. Do post work basing on the result
      // Currently nothing to do here

      // 4. Close the resources
      SysUtils.closeIgnoringExceptions(statemgr);
    }

    return isSuccessful;
  }

  protected boolean validateRuntimeManage(
      SchedulerStateManagerAdaptor adaptor,
      String topologyName) {
    // Check whether the topology has already been running
    Boolean isTopologyRunning = adaptor.isTopologyRunning(topologyName);

    if (isTopologyRunning == null || isTopologyRunning.equals(Boolean.FALSE)) {
      LOG.severe("No such topology exists");
      return false;
    }

    // Check whether cluster/role/environ matched
    ExecutionEnvironment.ExecutionState executionState = adaptor.getExecutionState(topologyName);
    if (executionState == null
        || !executionState.getCluster().equals(Context.cluster(config))
        || !executionState.getRole().equals(Context.role(config))
        || !executionState.getEnviron().equals(Context.environ(config))) {
      LOG.severe("cluster/role/environ not matched");
      return false;
    }

    return true;
  }

  protected boolean callRuntimeManagerRunner(Config runtime, ISchedulerClient schedulerClient) {
    // create an instance of the runner class
    RuntimeManagerRunner runtimeManagerRunner =
        new RuntimeManagerRunner(config, runtime, command, schedulerClient);

    // invoke the appropriate handlers based on command
    return runtimeManagerRunner.call();
  }

  protected ISchedulerClient getSchedulerClient(Config runtime) {
    return new SchedulerClientFactory(config, runtime).getSchedulerClient();
  }
}
