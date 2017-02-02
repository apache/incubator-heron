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
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.common.basics.DryRunFormatType;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.scheduler.dryrun.UpdateDryRunResponse;
import com.twitter.heron.scheduler.dryrun.UpdateRawDryRunRenderer;
import com.twitter.heron.scheduler.dryrun.UpdateTableDryRunRenderer;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.scheduler.SchedulerException;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TMasterException;

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

    Option dryRun = Option.builder("u")
        .desc("run in dry-run mode")
        .longOpt("dry_run")
        .required(false)
        .build();

    Option dryRunFormat = Option.builder("t")
        .desc("dry-run format")
        .longOpt("dry_run_format")
        .hasArg()
        .required(false)
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
    options.addOption(dryRun);
    options.addOption(dryRunFormat);
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

    Boolean dryRun = false;
    if (cmd.hasOption("u")) {
      dryRun = true;
    }

    // Default dry-run output format type
    DryRunFormatType dryRunFormat = DryRunFormatType.TABLE;
    if (cmd.hasOption("t")) {
      String format = cmd.getOptionValue("dry_run_format");
      dryRunFormat = DryRunFormatType.getDryRunFormatType(format);
      LOG.fine(String.format("Running dry-run mode using format %s", format));
    }

    Command command = Command.makeCommand(commandOption);

    // add config parameters from the command line
    Config.Builder commandLineConfig = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.dryRun(), dryRun)
        .put(Keys.dryRunFormat(), dryRunFormat)
        .put(Keys.verbose(), verbose)
        .put(Keys.topologyContainerId(), containerId);

    // This is a command line option, but not a valid config key. Hence we don't use Keys
    if (componentParallelism != null) {
      commandLineConfig.put(
          RuntimeManagerRunner.NEW_COMPONENT_PARALLELISM_KEY, componentParallelism);
    }

    Config.Builder topologyConfig = Config.newBuilder()
        .put(Keys.topologyName(), topologyName);

    // build the final config by expanding all the variables
    Config config = Config.expand(Config.newBuilder()
        .putAll(ClusterConfig.loadConfig(heronHome, configPath, releaseFile, overrideConfigFile))
        .putAll(commandLineConfig.build())
        .putAll(topologyConfig.build())
        .build());

    LOG.fine("Static config loaded successfully ");
    LOG.fine(config.toString());

    /* Meaning of exit status code:
      - status code = 0:
        program exits without error
      - 0 < status code < 100:
        program fails to execute before program execution. For example,
        JVM cannot find or load main class
      - 100 <= status code < 200:
        program fails to launch after program execution. For example,
        topology definition file fails to be loaded
      - status code == 200
        program sends out dry-run response */
    /* Since only stderr is used (by logging), we use stdout here to
       propagate any message back to Python's executor.py (invoke site). */
    // Create a new instance of RuntimeManagerMain
    RuntimeManagerMain runtimeManagerMain = new RuntimeManagerMain(config, command);
    try {
      runtimeManagerMain.manageTopology();
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (UpdateDryRunResponse response) {
      LOG.log(Level.FINE, "Sending out dry-run response");
      // Output may contain UTF-8 characters, so we should print using UTF-8 encoding
      PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
      out.print(runtimeManagerMain.renderDryRunResponse(response));
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      // Exit with status code 200 to indicate dry-run response is sent out
      System.exit(200);
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      LOG.log(Level.FINE, "Exception when submitting topology", e);
      System.out.println(e.getMessage());
      // Exit with status code 100 to indicate that error has happened on user-land
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(100);
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
   */
  public void manageTopology()
      throws TopologyRuntimeManagementException, TMasterException, PackingException {
    String topologyName = Context.topologyName(config);
    // 1. Do prepare work
    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr;
    try {
      statemgr = ReflectionUtils.newInstance(statemgrClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new TopologyRuntimeManagementException(String.format(
          "Failed to instantiate state manager class '%s'",
          statemgrClass), e);
    }

    // Put it in a try block so that we can always clean resources
    try {
      // initialize the statemgr
      statemgr.initialize(config);

      // TODO(mfu): timeout should read from config
      SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);

      validateRuntimeManage(adaptor, topologyName);

      // 2. Try to manage topology if valid
      // invoke the appropriate command to manage the topology
      LOG.log(Level.FINE, "Topology: {0} to be {1}ed", new Object[]{topologyName, command});

      // build the runtime config
      Config runtime = Config.newBuilder()
          .put(Keys.topologyName(), Context.topologyName(config))
          .put(Keys.schedulerStateManagerAdaptor(), adaptor)
          .build();

      // Create a ISchedulerClient basing on the config
      ISchedulerClient schedulerClient = getSchedulerClient(runtime);

      callRuntimeManagerRunner(runtime, schedulerClient);
    } finally {
      // 3. Do post work basing on the result
      // Currently nothing to do here

      // 4. Close the resources
      SysUtils.closeIgnoringExceptions(statemgr);
    }
  }

  protected void validateRuntimeManage(
      SchedulerStateManagerAdaptor adaptor,
      String topologyName) throws TopologyRuntimeManagementException {
    // Check whether the topology has already been running
    Boolean isTopologyRunning = adaptor.isTopologyRunning(topologyName);

    if (isTopologyRunning == null || isTopologyRunning.equals(Boolean.FALSE)) {
      throw new TopologyRuntimeManagementException(
          String.format("Topology '%s' does not exist", topologyName));
    }

    // Check whether cluster/role/environ matched
    ExecutionEnvironment.ExecutionState executionState = adaptor.getExecutionState(topologyName);
    if (executionState == null) {
      throw new TopologyRuntimeManagementException(
          String.format("Failed to get execution state for topology %s", topologyName));
    }

    String stateCluster = executionState.getCluster();
    String stateRole = executionState.getRole();
    String stateEnv = executionState.getEnviron();
    String configCluster = Context.cluster(config);
    String configRole = Context.role(config);
    String configEnv = Context.environ(config);
    if (!stateCluster.equals(configCluster)
        || !stateRole.equals(configRole)
        || !stateEnv.equals(configEnv)) {
      String currentState = String.format("%s/%s/%s", stateCluster, stateRole, stateEnv);
      String configState = String.format("%s/%s/%s", configCluster, configRole, configEnv);
      throw new TopologyRuntimeManagementException(String.format(
          "cluster/role/environ does not match. Topology '%s' is running at %s, not %s",
          topologyName, currentState, configState));
    }
  }

  protected void callRuntimeManagerRunner(Config runtime, ISchedulerClient schedulerClient)
    throws TopologyRuntimeManagementException, TMasterException, PackingException {
    // create an instance of the runner class
    RuntimeManagerRunner runtimeManagerRunner =
        new RuntimeManagerRunner(config, runtime, command, schedulerClient);

    // invoke the appropriate handlers based on command
    runtimeManagerRunner.call();
  }

  protected ISchedulerClient getSchedulerClient(Config runtime)
      throws SchedulerException {
    return new SchedulerClientFactory(config, runtime).getSchedulerClient();
  }

  protected String renderDryRunResponse(UpdateDryRunResponse resp) {
    DryRunFormatType formatType = Context.dryRunFormatType(config);
    switch (formatType) {
      case RAW :
        return new UpdateRawDryRunRenderer(resp).render();
      case TABLE:
        return new UpdateTableDryRunRenderer(resp).render();
      default: throw new IllegalArgumentException(
          String.format("Unexpected rendering format: %s", formatType));
    }
  }
}
