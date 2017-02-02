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

import java.io.PrintStream;
import java.net.URI;
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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.DryRunFormatType;
import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.scheduler.dryrun.SubmitDryRunResponse;
import com.twitter.heron.scheduler.dryrun.SubmitRawDryRunRenderer;
import com.twitter.heron.scheduler.dryrun.SubmitTableDryRunRenderer;
import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.LauncherException;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.uploader.UploaderException;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * Calls Uploader to upload topology package, and Launcher to launch Scheduler.
 */
public class SubmitterMain {
  private static final Logger LOG = Logger.getLogger(SubmitterMain.class.getName());

  /**
   * Load the topology config
   *
   * @param topologyPackage, tar ball containing user submitted jar/tar, defn and config
   * @param topologyBinaryFile, name of the user submitted topology jar/tar/pex file
   * @param topology, proto in memory version of topology definition
   * @return config, the topology config
   */
  protected static Config topologyConfigs(
      String topologyPackage, String topologyBinaryFile, String topologyDefnFile,
      TopologyAPI.Topology topology) {
    PackageType packageType = PackageType.getPackageType(topologyBinaryFile);

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinitionFile(), topologyDefnFile)
        .put(Keys.topologyPackageFile(), topologyPackage)
        .put(Keys.topologyBinaryFile(), topologyBinaryFile)
        .put(Keys.topologyPackageType(), packageType)
        .build();
    return config;
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @param verbose, enable verbose logging
   * @return config, the command line config
   */
  protected static Config commandLineConfigs(String cluster,
                                             String role,
                                             String environ,
                                             Boolean dryRun,
                                             DryRunFormatType dryRunFormat,
                                             Boolean verbose) {
    Config config = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.dryRun(), dryRun)
        .put(Keys.dryRunFormat(), dryRunFormat)
        .put(Keys.verbose(), verbose)
        .build();

    return config;
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("SubmitterMain", options);
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

    Option heronHome = Option.builder("d")
        .desc("Directory where heron is installed")
        .longOpt("heron_home")
        .hasArgs()
        .argName("heron home dir")
        .required()
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

    Option topologyPackage = Option.builder("y")
        .desc("tar ball containing user submitted jar/tar, defn and config")
        .longOpt("topology_package")
        .hasArgs()
        .argName("topology package")
        .required()
        .build();

    Option topologyDefn = Option.builder("f")
        .desc("serialized file containing Topology protobuf")
        .longOpt("topology_defn")
        .hasArgs()
        .argName("topology definition")
        .required()
        .build();

    Option topologyJar = Option.builder("j")
        .desc("user heron topology jar/pex file path")
        .longOpt("topology_bin")
        .hasArgs()
        .argName("topology binary file")
        .required()
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
    options.addOption(heronHome);
    options.addOption(configFile);
    options.addOption(configOverrides);
    options.addOption(releaseFile);
    options.addOption(topologyPackage);
    options.addOption(topologyDefn);
    options.addOption(topologyJar);
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

  public static void main(String[] args) throws Exception {
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
    String topologyPackage = cmd.getOptionValue("topology_package");
    String topologyDefnFile = cmd.getOptionValue("topology_defn");
    String topologyBinaryFile = cmd.getOptionValue("topology_bin");

    // load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    Boolean dryRun = false;
    if (cmd.hasOption("u")) {
      dryRun = true;
    }

    // Default dry-run output format type
    DryRunFormatType dryRunFormat = DryRunFormatType.TABLE;
    if (cmd.hasOption("f")) {
      String format = cmd.getOptionValue("dry_run_format");
      dryRunFormat = DryRunFormatType.getDryRunFormatType(format);
      LOG.fine(String.format("Running dry-run mode using format %s", format));
    }

    // first load the defaults, then the config from files to override it
    // next add config parameters from the command line
    // load the topology configs

    // build the final config by expanding all the variables
    Config config = Config.expand(Config.newBuilder()
        .putAll(ClusterConfig.loadConfig(heronHome, configPath, releaseFile, overrideConfigFile))
        .putAll(commandLineConfigs(cluster, role, environ, dryRun, dryRunFormat, verbose))
        .putAll(topologyConfigs(topologyPackage, topologyBinaryFile, topologyDefnFile, topology))
        .build());

    LOG.fine("Static config loaded successfully");
    LOG.fine(config.toString());

    SubmitterMain submitterMain = new SubmitterMain(config, topology);
    /* Meaning of exit status code:
       - status code = 0:
         program exits without error
       - 0 < status code < 100:
         program fails to execute before program execution. For example,
         JVM cannot find or load main class
       - 100 <= status code < 200:
         program fails to launch after program execution. For example,
         topology definition file fails to be loaded
       - status code >= 200
         program sends out dry-run response */
    try {
      submitterMain.submitTopology();
    } catch (SubmitDryRunResponse response) {
      LOG.log(Level.FINE, "Sending out dry-run response");
      // Output may contain UTF-8 characters, so we should print using UTF-8 encoding
      PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
      out.print(submitterMain.renderDryRunResponse(response));
      // Exit with status code 200 to indicate dry-run response is sent out
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(200);
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      /* Since only stderr is used (by logging), we use stdout here to
         propagate error message back to Python's executor.py (invoke site). */
      LOG.log(Level.FINE, "Exception when submitting topology", e);
      System.out.println(e.getMessage());
      // Exit with status code 100 to indicate that error has happened on user-land
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(100);
    }
    LOG.log(Level.FINE, "Topology {0} submitted successfully", topology.getName());
  }

  // holds all the config read
  private final Config config;

  // topology definition
  private final TopologyAPI.Topology topology;

  public SubmitterMain(
      Config config,
      TopologyAPI.Topology topology) {
    // initialize the options
    this.config = config;
    this.topology = topology;
  }

  /**
   * Submit a topology
   * 1. Instantiate necessary resources
   * 2. Valid whether it is legal to submit a topology
   * 3. Call LauncherRunner
   *
   */
  public void submitTopology() throws TopologySubmissionException {
    // 1. Do prepare work
    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr;

    // Create an instance of the launcher class
    String launcherClass = Context.launcherClass(config);
    ILauncher launcher;

    // create an instance of the uploader class
    String uploaderClass = Context.uploaderClass(config);
    IUploader uploader;

    // create an instance of state manager
    try {
      statemgr = ReflectionUtils.newInstance(statemgrClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new TopologySubmissionException(
          String.format("Failed to instantiate state manager class '%s'", statemgrClass), e);
    }

    // create an instance of launcher
    try {
      launcher = ReflectionUtils.newInstance(launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // create an instance of uploader
    try {
      uploader = ReflectionUtils.newInstance(uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new UploaderException(
          String.format("Failed to instantiate uploader class '%s'", uploaderClass), e);
    }

    // Put it in a try block so that we can always clean resources
    try {
      // initialize the state manager
      statemgr.initialize(config);

      // TODO(mfu): timeout should read from config
      SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);

      // Build the basic runtime config
      Config runtime = Config.newBuilder()
          .putAll(LauncherUtils.getInstance().getPrimaryRuntime(topology, adaptor)).build();

      // Bypass validation and upload if in dry-run mode
      if (Context.dryRun(config)) {
        callLauncherRunner(runtime);
      } else {

        // Check if topology is already running
        validateSubmit(adaptor, topology.getName());

        LOG.log(Level.FINE, "Topology {0} to be submitted", topology.getName());

        // Try to submit topology if valid
        // Firstly, try to upload necessary packages
        URI packageURI = uploadPackage(uploader);

        // Secondly, try to submit the topology
        // build the complete runtime config
        Config runtimeAll = Config.newBuilder()
            .putAll(runtime)
            .put(Keys.topologyPackageUri(), packageURI)
            .put(Keys.launcherClassInstance(), launcher)
            .build();
        callLauncherRunner(runtimeAll);
      }
    } catch (LauncherException | PackingException e) {
      // we undo uploading of topology package only if launcher fails to
      // launch topology, which will throw LauncherException or PackingException
      uploader.undo();
      throw e;
    } finally {
      SysUtils.closeIgnoringExceptions(uploader);
      SysUtils.closeIgnoringExceptions(launcher);
      SysUtils.closeIgnoringExceptions(statemgr);
    }
  }

  protected void validateSubmit(SchedulerStateManagerAdaptor adaptor, String topologyName)
      throws TopologySubmissionException {
    // Check whether the topology has already been running
    // TODO(rli): anti-pattern is too nested on this path to be refactored
    Boolean isTopologyRunning = adaptor.isTopologyRunning(topologyName);

    if (isTopologyRunning != null && isTopologyRunning.equals(Boolean.TRUE)) {
      throw new TopologySubmissionException(
          String.format("Topology '%s' already exists", topologyName));
    }
  }

  protected URI uploadPackage(IUploader uploader) throws UploaderException {
    // initialize the uploader
    uploader.initialize(config);

    // upload the topology package to the storage
    return uploader.uploadPackage();
  }

  protected void callLauncherRunner(Config runtime)
      throws LauncherException, PackingException, SubmitDryRunResponse {
    // using launch runner, launch the topology
    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    launchRunner.call();
  }

  protected String renderDryRunResponse(SubmitDryRunResponse resp) {
    DryRunFormatType formatType = Context.dryRunFormatType(config);
    switch (formatType) {
      case RAW : return new SubmitRawDryRunRenderer(resp).render();
      case TABLE: return new SubmitTableDryRunRenderer(resp).render();
      default: throw new IllegalArgumentException(
          String.format("Unexpected rendering format: %s", formatType));
    }
  }
}
