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

package org.apache.heron.scheduler;

import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.Slf4jUtils;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.DryRunFormatType;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.utils.logging.LoggingHelper;
import org.apache.heron.scheduler.dryrun.SubmitDryRunResponse;
import org.apache.heron.scheduler.utils.DryRunRenders;
import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.scheduler.utils.SubmitterUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.ConfigLoader;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.ILauncher;
import org.apache.heron.spi.scheduler.LauncherException;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.uploader.IUploader;
import org.apache.heron.spi.uploader.UploaderException;
import org.apache.heron.spi.utils.ReflectionUtils;

/**
 * Calls Uploader to upload topology package, and Launcher to launch Scheduler.
 */
public class SubmitterMain {
  private static final Logger LOG = Logger.getLogger(SubmitterMain.class.getName());

  /**
   * Load the config parameters from the command line
   *
   * @param cluster name of the cluster
   * @param role user role
   * @param environ user provided environment/tag
   * @param submitUser the submit user
   * @param dryRun run as dry run
   * @param dryRunFormat the dry run format
   * @param verbose enable verbose logging
   * @param verboseGC enable verbose JVM GC logging
   * @return config the command line config
   */
  protected static Config commandLineConfigs(String cluster,
                                             String role,
                                             String environ,
                                             String submitUser,
                                             Boolean dryRun,
                                             DryRunFormatType dryRunFormat,
                                             Boolean verbose,
                                             Boolean verboseGC) {
    return Config.newBuilder()
        .put(Key.CLUSTER, cluster)
        .put(Key.ROLE, role)
        .put(Key.ENVIRON, environ)
        .put(Key.SUBMIT_USER, submitUser)
        .put(Key.DRY_RUN, dryRun)
        .put(Key.DRY_RUN_FORMAT_TYPE, dryRunFormat)
        .put(Key.VERBOSE, verbose)
        .put(Key.VERBOSE_GC, verboseGC)
        .build();
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

    Option submitUser = Option.builder("s")
        .desc("User submitting the topology")
        .longOpt("submit_user")
        .hasArgs()
        .argName("submit userid")
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
        .desc("The filename of the heron topology jar/tar/pex file to be run by the executor")
        .longOpt("topology_bin")
        .hasArgs()
        .argName("topology binary filename on the cluster")
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

    Option verboseGC = Option.builder("vgc")
            .desc("Enable verbose JVM GC logs")
            .longOpt("verbose_gc")
            .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(submitUser);
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
    options.addOption(verboseGC);

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

  private static boolean isVerbose(CommandLine cmd) {
    return cmd.hasOption("v");
  }

  private static boolean isVerboseGC(CommandLine cmd) {
    return cmd.hasOption("vgc");
  }

  @SuppressWarnings("JavadocMethod")
  @VisibleForTesting
  public static Config loadConfig(CommandLine cmd, TopologyAPI.Topology topology) {
    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    String submitUser = cmd.getOptionValue("submit_user");
    String heronHome = cmd.getOptionValue("heron_home");
    String configPath = cmd.getOptionValue("config_path");
    String overrideConfigFile = cmd.getOptionValue("override_config_file");
    String releaseFile = cmd.getOptionValue("release_file");
    String topologyPackage = cmd.getOptionValue("topology_package");
    String topologyDefnFile = cmd.getOptionValue("topology_defn");
    String topologyBinaryFile = cmd.getOptionValue("topology_bin");

    Boolean dryRun = false;
    if (cmd.hasOption("u")) {
      dryRun = true;
    }

    // Default dry-run output format type
    DryRunFormatType dryRunFormat = DryRunFormatType.TABLE;
    if (dryRun && cmd.hasOption("t")) {
      String format = cmd.getOptionValue("dry_run_format");
      dryRunFormat = DryRunFormatType.getDryRunFormatType(format);
      LOG.fine(String.format("Running dry-run mode using format %s", format));
    }

    // first load the defaults, then the config from files to override it
    // next add config parameters from the command line
    // load the topology configs

    // build the final config by expanding all the variables
    return Config.toLocalMode(Config.newBuilder()
        .putAll(ConfigLoader.loadConfig(heronHome, configPath, releaseFile, overrideConfigFile))
        .putAll(commandLineConfigs(cluster, role, environ, submitUser, dryRun,
            dryRunFormat, isVerbose(cmd), isVerboseGC(cmd)))
        .putAll(SubmitterUtils.topologyConfigs(topologyPackage, topologyBinaryFile,
            topologyDefnFile, topology))
        .build());
  }

  public static void main(String[] args) throws Exception {
    Slf4jUtils.installSLF4JBridge();
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

    Level logLevel = Level.INFO;
    if (isVerbose(cmd)) {
      logLevel = Level.ALL;
    }

    // init log
    LoggingHelper.loggerInit(logLevel, false);

    // load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(cmd.getOptionValue("topology_defn"));
    Config config = loadConfig(cmd, topology);

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
      out.print(DryRunRenders.render(response, Context.dryRunFormatType(config)));
      // Exit with status code 200 to indicate dry-run response is sent out
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(200);
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      /* Since only stderr is used (by logging), we use stdout here to
         propagate error message back to Python's executor.py (invoke site). */
      LOG.log(Level.SEVERE, "Exception when submitting topology", e);
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

  public SubmitterMain(Config config, TopologyAPI.Topology topology) {
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
    // build primary runtime config first
    Config primaryRuntime = Config.newBuilder()
          .putAll(LauncherUtils.getInstance().createPrimaryRuntime(topology)).build();
    // call launcher directly here if in dry-run mode
    if (Context.dryRun(config)) {
      callLauncherRunner(primaryRuntime);
      return;
    }
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

      // initialize the uploader
      uploader.initialize(config);

      // TODO(mfu): timeout should read from config
      SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);

      // Check if topology is already running
      validateSubmit(adaptor, topology.getName());

      LOG.log(Level.FINE, "Topology {0} to be submitted", topology.getName());

      Config runtimeWithoutPackageURI = Config.newBuilder()
          .putAll(primaryRuntime)
          .putAll(LauncherUtils.getInstance().createAdaptorRuntime(adaptor))
          .put(Key.LAUNCHER_CLASS_INSTANCE, launcher)
          .build();

      PackingPlan packingPlan = LauncherUtils.getInstance()
          .createPackingPlan(config, runtimeWithoutPackageURI);

      // The packing plan might call for a number of containers different than the config
      // settings. If that's the case we need to modify the configs to match.
      runtimeWithoutPackageURI =
          updateNumContainersIfNeeded(runtimeWithoutPackageURI, topology, packingPlan);

      // If the packing plan is valid we will upload necessary packages
      URI packageURI = uploadPackage(uploader);

      // Update the runtime config with the packageURI
      Config runtimeAll = Config.newBuilder()
          .putAll(runtimeWithoutPackageURI)
          .put(Key.TOPOLOGY_PACKAGE_URI, packageURI)
          .build();

      callLauncherRunner(runtimeAll);

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

  /**
   * Checks that the number of containers specified in the topology matches the number of containers
   * called for in the packing plan. If they are different, returns a new config with settings
   * updated to align with the packing plan. The new config will include an updated
   * Key.TOPOLOGY_DEFINITION containing a cloned Topology with it's settings also updated.
   *
   * @param initialConfig initial config to clone and update (if necessary)
   * @param initialTopology topology to check and clone/update (if necessary)
   * @param packingPlan packing plan to compare settings with
   * @return a new Config cloned from initialConfig and modified as needed to align with packedPlan
   */
  @VisibleForTesting
  Config updateNumContainersIfNeeded(Config initialConfig,
                                     TopologyAPI.Topology initialTopology,
                                     PackingPlan packingPlan) {

    int configNumStreamManagers = TopologyUtils.getNumContainers(initialTopology);
    int packingNumStreamManagers = packingPlan.getContainers().size();

    if (configNumStreamManagers == packingNumStreamManagers) {
      return initialConfig;
    }

    Config.Builder newConfigBuilder = Config.newBuilder()
        .putAll(initialConfig)
        .put(Key.NUM_CONTAINERS, packingNumStreamManagers + 1)
        .put(Key.TOPOLOGY_DEFINITION,
            cloneWithNewNumContainers(initialTopology, packingNumStreamManagers));

    String packingClass = Context.packingClass(config);
    LOG.warning(String.format("The packing plan (generated by %s) calls for a different number of "
            + "containers (%d) than what was explicitly set in the topology configs (%d). "
            + "Overriding the configs to specify %d containers. When using %s do not explicitly "
            + "call config.setNumStmgrs(..) or config.setNumWorkers(..).",
        packingClass, packingNumStreamManagers, configNumStreamManagers,
        packingNumStreamManagers, packingClass));

    return newConfigBuilder.build();
  }

  private TopologyAPI.Topology cloneWithNewNumContainers(TopologyAPI.Topology initialTopology,
                                                         int numStreamManagers) {
    TopologyAPI.Topology.Builder topologyBuilder = TopologyAPI.Topology.newBuilder(initialTopology);
    TopologyAPI.Config.Builder configBuilder = TopologyAPI.Config.newBuilder();

    for (TopologyAPI.Config.KeyValue keyValue : initialTopology.getTopologyConfig().getKvsList()) {

      // override TOPOLOGY_STMGRS value once we find it
      if (org.apache.heron.api.Config.TOPOLOGY_STMGRS.equals(keyValue.getKey())) {
        TopologyAPI.Config.KeyValue.Builder kvBuilder = TopologyAPI.Config.KeyValue.newBuilder();
        kvBuilder.setKey(keyValue.getKey());
        kvBuilder.setValue(Integer.toString(numStreamManagers));
        configBuilder.addKvs(kvBuilder.build());
      } else {
        configBuilder.addKvs(keyValue);
      }
    }

    return topologyBuilder.setTopologyConfig(configBuilder).build();
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
    // upload the topology package to the storage
    return uploader.uploadPackage();
  }

  protected void callLauncherRunner(Config runtime)
      throws LauncherException, PackingException, SubmitDryRunResponse {
    // using launch runner, launch the topology
    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    launchRunner.call();
  }
}
