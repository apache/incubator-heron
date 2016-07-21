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

import java.net.URI;
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
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.uploader.IUploader;
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
   * @param topologyJarFile, name of the user submitted topology jar/tar file
   * @param topology, proto in memory version of topology definition
   * @return config, the topology config
   */
  protected static SpiCommonConfig topologyConfigs(
      String topologyPackage, String topologyJarFile, String topologyDefnFile,
      TopologyAPI.Topology topology) {

    String pkgType = FileUtils.isOriginalPackageJar(
        FileUtils.getBaseName(topologyJarFile)) ? "jar" : "tar";

    SpiCommonConfig config = SpiCommonConfig.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinitionFile(), topologyDefnFile)
        .put(Keys.topologyPackageFile(), topologyPackage)
        .put(Keys.topologyJarFile(), topologyJarFile)
        .put(Keys.topologyPackageType(), pkgType)
        .build();
    return config;
  }

  /**
   * Load the defaults config
   *
   * @param heronHome, directory of heron home
   * @param configPath, directory containing the config
   * @param releaseFile, release file containing build information
   * <p>
   * return config, the defaults config
   */
  protected static SpiCommonConfig defaultConfigs(String heronHome, String configPath, String releaseFile) {
    SpiCommonConfig config = SpiCommonConfig.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, configPath, releaseFile))
        .build();
    return config;
  }

  /**
   * Load the override config from cli
   *
   * @param overrideConfigPath, override config file path
   * <p>
   * @return config, the override config
   */
  protected static SpiCommonConfig overrideConfigs(String overrideConfigPath) {
    SpiCommonConfig config = SpiCommonConfig.newBuilder()
        .putAll(ClusterConfig.loadOverrideConfig(overrideConfigPath))
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
  protected static SpiCommonConfig commandLineConfigs(String cluster,
                                             String role,
                                             String environ,
                                             Boolean verbose) {
    SpiCommonConfig config = SpiCommonConfig.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
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
        .desc("user heron topology jar")
        .longOpt("topology_jar")
        .hasArgs()
        .argName("topology jar")
        .required()
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
    String topologyJarFile = cmd.getOptionValue("topology_jar");

    // load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    // first load the defaults, then the config from files to override it
    // next add config parameters from the command line
    // load the topology configs

    // build the final config by expanding all the variables
    SpiCommonConfig config = SpiCommonConfig.expand(
        SpiCommonConfig.newBuilder()
            .putAll(defaultConfigs(heronHome, configPath, releaseFile))
            .putAll(overrideConfigs(overrideConfigFile))
            .putAll(commandLineConfigs(cluster, role, environ, verbose))
            .putAll(topologyConfigs(
                topologyPackage, topologyJarFile, topologyDefnFile, topology))
            .build());

    LOG.fine("Static config loaded successfully ");
    LOG.fine(config.toString());

    SubmitterMain submitterMain = new SubmitterMain(config, topology);
    boolean isSuccessful = submitterMain.submitTopology();

    // Log the result and exit
    if (!isSuccessful) {
      throw new RuntimeException(String.format("Failed to submit topology %s", topology.getName()));
    } else {
      LOG.log(Level.FINE, "Topology {0} submitted successfully", topology.getName());
    }
  }

  // holds all the config read
  private final SpiCommonConfig config;

  // topology definition
  private final TopologyAPI.Topology topology;

  public SubmitterMain(
      SpiCommonConfig config,
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
   * @return true if the topology is submitted successfully
   */
  public boolean submitTopology() {
    // 1. Do prepare work
    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr;

    // Create an instance of the launcher class
    String launcherClass = Context.launcherClass(config);
    ILauncher launcher;

    // Create an instance of the packing class
    String packingClass = Context.packingClass(config);
    IPacking packing;

    // create an instance of the uploader class
    String uploaderClass = Context.uploaderClass(config);
    IUploader uploader;

    try {
      // create an instance of state manager
      statemgr = ReflectionUtils.newInstance(statemgrClass);

      // create an instance of launcher
      launcher = ReflectionUtils.newInstance(launcherClass);

      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(packingClass);

      // create an instance of uploader
      uploader = ReflectionUtils.newInstance(uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.log(Level.SEVERE, "Failed to instantiate instances", e);
      return false;
    }

    boolean isSuccessful = false;
    URI packageURI = null;
    // Put it in a try block so that we can always clean resources
    try {
      // initialize the state manager
      statemgr.initialize(config);

      // TODO(mfu): timeout should read from config
      SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);

      boolean isValid = validateSubmit(adaptor, topology.getName());

      // 2. Try to submit topology if valid
      if (isValid) {
        // invoke method to submit the topology
        LOG.log(Level.FINE, "Topology {0} to be submitted", topology.getName());

        // Firstly, try to upload necessary packages
        packageURI = uploadPackage(uploader);
        if (packageURI == null) {
          LOG.severe("Failed to upload package.");
          return false;
        } else {
          // Secondly, try to submit a topology
          // build the runtime config
          SpiCommonConfig runtime = SpiCommonConfig.newBuilder()
              .put(Keys.topologyId(), topology.getId())
              .put(Keys.topologyName(), topology.getName())
              .put(Keys.topologyDefinition(), topology)
              .put(Keys.schedulerStateManagerAdaptor(), adaptor)
              .put(Keys.numContainers(), 1 + TopologyUtils.getNumContainers(topology))
              .put(Keys.topologyPackageUri(), packageURI)
              .put(Keys.launcherClassInstance(), launcher)
              .put(Keys.packingClassInstance(), packing)
              .build();

          isSuccessful = callLauncherRunner(runtime);
        }
      }
    } finally {
      // 3. Do post work basing on the result
      if (!isSuccessful) {
        // Undo the upload if failed to submit && the upload is successful
        if (packageURI != null) {
          uploader.undo();
        }
      }

      // 4. Close the resources
      SysUtils.closeIgnoringExceptions(uploader);
      SysUtils.closeIgnoringExceptions(packing);
      SysUtils.closeIgnoringExceptions(launcher);
      SysUtils.closeIgnoringExceptions(statemgr);
    }

    return isSuccessful;
  }

  protected boolean validateSubmit(SchedulerStateManagerAdaptor adaptor, String topologyName) {
    // Check whether the topology has already been running
    Boolean isTopologyRunning = adaptor.isTopologyRunning(topologyName);

    if (isTopologyRunning != null && isTopologyRunning.equals(Boolean.TRUE)) {
      LOG.severe("Topology already exists");
      return false;
    }

    return true;
  }

  protected URI uploadPackage(IUploader uploader) {
    // initialize the uploader
    uploader.initialize(config);

    // upload the topology package to the storage
    URI uploaderRet = uploader.uploadPackage();

    return uploaderRet;
  }

  protected boolean callLauncherRunner(SpiCommonConfig runtime) {
    // using launch runner, launch the topology
    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    boolean result = launchRunner.call();

    // if failed, undo the uploaded package
    if (!result) {
      LOG.severe("Failed to launch topology. Attempting to roll back upload.");
      return false;
    }
    return true;
  }
}
