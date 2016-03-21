package com.twitter.heron.scheduler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

/**
 * Calls Uploader to upload topology package, and Launcher to launch Scheduler.
 * TODO(nbhagat): Use commons cli to parse command line.
 * TODO(nbhagat): Make all argument passed to CLI available here.
 * args[0] = Topology Package location. Topology package generated from heron-cli will be a
 * tar file containing topology, topology definition and all dependencies.
 * args[1] = ConfigLoader class.
 * args[2] = Scheduler config file.
 * args[3] = Config override string encoded as base64 string.
 * args[4] = topology definition file.
 * args[5] = Heron internals config file location.
 * args[6] = Original package.
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
  protected static Config topologyConfigs(
      String topologyPackage, String topologyJarFile, String topologyDefnFile,
      TopologyAPI.Topology topology) {

    String pkgType = FileUtils.isOriginalPackageJar(
        FileUtils.getBaseName(topologyJarFile)) ? "jar" : "tar";

    Config config = Config.newBuilder()
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
   * <p/>
   * return config, the defaults config
   */
  protected static Config defaultConfigs(String heronHome, String configPath) {
    Config config = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, configPath))
        .build();
    return config;
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @return config, the command line config
   */
  protected static Config commandLineConfigs(String cluster, String role, String environ) {
    Config config = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .build();
    return config;
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "SubmitterMain", options );
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
        .desc("Diretory where heron is installed")
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

    // TODO: Need to figure out the exact format
    Option configOverrides = Option.builder("o")
        .desc("Command line config overrides")
        .longOpt("config_overrides")
        .hasArgs()
        .argName("config overrides")
        .build();

    Option topologyPackage = Option.builder("y")
        .desc("tar ball containing user submitted jar/tar, defn and config")
        .longOpt("topology_package")
        .hasArgs()
        .argName("topology package")
        .build();

    Option topologyDefn = Option.builder("f")
        .desc("serialized file containing Topology protobuf")
        .longOpt("topology_defn")
        .hasArgs()
        .argName("topology definition")
        .build();

    Option topologyJar = Option.builder("j")
        .desc("user heron topology jar")
        .longOpt("topology_jar")
        .hasArgs()
        .argName("topology jar")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(heronHome);
    options.addOption(configFile);
    options.addOption(configOverrides);
    options.addOption(topologyPackage);
    options.addOption(topologyDefn);
    options.addOption(topologyJar);

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

  public static void main(String[] args) throws
      ClassNotFoundException, InstantiationException,
      IllegalAccessException, IOException, ParseException {

    Options options = constructOptions();
    Options helpOptions = constructHelpOptions();
    CommandLineParser parser = new DefaultParser();
    // parse the help options first.
    CommandLine cmd = parser.parse(helpOptions, args, true);;

    if(cmd.hasOption("h")) {
      usage(options);
      return;
    }

    try {
      // Now parse the required options
      cmd = parser.parse(options, args);
    } catch(ParseException e) {
      LOG.severe("Error parsing command line options: " + e.getMessage());
      usage(options);
      System.exit(1);
    }

    String cluster = cmd.getOptionValue("cluster");;
    String role = cmd.getOptionValue("role");;
    String environ = cmd.getOptionValue("environment");;
    String heronHome = cmd.getOptionValue("heron_home");;
    String configPath = cmd.getOptionValue("config_path");;
    String configOverrideEncoded = cmd.getOptionValue("config_overrides");;

    String topologyPackage = cmd.getOptionValue("topology_package");;
    String topologyDefnFile = cmd.getOptionValue("topology_defn");;
    String topologyJarFile = cmd.getOptionValue("topology_jar");;

    System.out.println(heronHome + " " + configPath);

    // load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    // first load the defaults, then the config from files to override it
    // next add config parameters from the command line
    // load the topology configs
    // TODO (Karthik) override any parameters from the command line

    // build the final config by expanding all the variables
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultConfigs(heronHome, configPath))
            .putAll(commandLineConfigs(cluster, role, environ))
            .putAll(topologyConfigs(
                topologyPackage, topologyJarFile, topologyDefnFile, topology))
            .build());

    LOG.info("Static config loaded successfully ");
    LOG.info(config.toString());

    // 1. Do prepare work
    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = (IStateManager) Class.forName(statemgrClass).newInstance();

    // Create an instance of the launcher class
    String launcherClass = Context.launcherClass(config);
    ILauncher launcher = (ILauncher) Class.forName(launcherClass).newInstance();

    // Create an instance of the packing class
    String packingClass = Context.packingClass(config);
    IPacking packing = (IPacking) Class.forName(packingClass).newInstance();

    // Local variable for convenient access
    String topologyName = topology.getName();

    boolean isSuccessful = false;
    UploadRunner uploadRunner = null;
    // Put it in a try block so that we can always clean resources
    try {
      // initialize the state manager
      statemgr.initialize(config);

      uploadRunner = new UploadRunner(config);

      boolean isValid = validateSubmit(statemgr, topologyName);

      // 2. Try to submit topology if valid
      if (isValid) {
        // invoke method to submit the topology
        LOG.log(Level.INFO, "Topology {0} to be submitted", topologyName);

        isSuccessful = submitTopology(config, topology, statemgr, launcher, packing, uploadRunner);
      }
    } finally {
      // 3. Do generic cleaning
      // close the state manager
      statemgr.close();

      // 4. Do post work basing on the result
      if (!isSuccessful) {
        // Undo if failed to submit
        if (uploadRunner != null) {
          uploadRunner.undo();
        }
        launcher.undo();
      }
    }

    // Log the result and exit
    if (!isSuccessful) {
      LOG.log(Level.SEVERE, "Failed to submit topology {0}. Exiting", topologyName);

      System.exit(1);
    } else {
      LOG.log(Level.INFO, "Topology {0} submitted successfully", topologyName);

      System.exit(0);
    }
  }

  public static boolean validateSubmit(IStateManager statemgr, String topologyName) {
    // Check whether the topology has already been running
    Boolean isTopologyRunning =
        NetworkUtils.awaitResult(statemgr.isTopologyRunning(topologyName), 5, TimeUnit.SECONDS);

    if (isTopologyRunning != null && isTopologyRunning.equals(Boolean.TRUE)) {
      LOG.severe("Topology already exists");
      return false;
    }

    return true;
  }

  public static boolean submitTopology(Config config, TopologyAPI.Topology topology,
                                       IStateManager statemgr, ILauncher launcher,
                                       IPacking packing, UploadRunner uploadRunner)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    // upload the topology package to the storage
    boolean result = uploadRunner.call();

    if (!result) {
      LOG.severe("Failed to upload package.");
      return false;
    }

    // build the runtime config
    Config runtime = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinition(), topology)
        .put(Keys.schedulerStateManagerAdaptor(), new SchedulerStateManagerAdaptor(statemgr))
        .put(Keys.topologyPackageUri(), uploadRunner.getUri())
        .put(Keys.launcherClassInstance(), launcher)
        .put(Keys.packingClassInstance(), packing)
        .build();

    // using launch runner, launch the topology
    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    result = launchRunner.call();

    // if failed, undo the uploaded package
    if (!result) {
      LOG.severe("Failed to launch topology. Attempting to roll back upload.");
      return false;
    }
    return true;
  }
}
