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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.server.SchedulerServer;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.Shutdown;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * Main class of scheduler.
 */
public class SchedulerMain {
  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  /**
   * Load the topology config
   *
   * @param topologyJarFile, name of the user submitted topology jar/tar file
   * @param topologyDefnFile, name of the topology defintion file
   * @param topology, proto in memory version of topology definition
   * @return config, the topology config
   */
  protected static Config topologyConfigs(
      String topologyJarFile, String topologyDefnFile, TopologyAPI.Topology topology) {

    String pkgType = FileUtils.isOriginalPackageJar(
        FileUtils.getBaseName(topologyJarFile)) ? "jar" : "tar";

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinitionFile(), topologyDefnFile)
        .put(Keys.topologyJarFile(), topologyJarFile)
        .put(Keys.topologyPackageType(), pkgType)
        .build();

    return config;
  }

  /**
   * Load the defaults config
   * <p/>
   * return config, the defaults config
   */
  protected static Config defaultConfigs() {
    Config config = Config.newBuilder()
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(ClusterConfig.loadSandboxConfig())
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
    formatter.printHelp("SchedulerMain", options);
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

    Option topologyJar = Option.builder("f")
        .desc("Topology jar file path")
        .longOpt("topology_jar")
        .hasArgs()
        .argName("topology jar file")
        .required()
        .build();

    Option schedulerHTTPPort = Option.builder("p")
        .desc("Http Port number on which the scheduler listens for requests")
        .longOpt("http_port")
        .hasArgs()
        .argName("http port")
        .required()
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(topologyName);
    options.addOption(topologyJar);
    options.addOption(schedulerHTTPPort);

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
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException {

    Options options = constructOptions();
    Options helpOptions = constructHelpOptions();
    CommandLineParser parser = new DefaultParser();
    // parse the help options first.
    CommandLine cmd = parser.parse(helpOptions, args, true);
    ;

    if (cmd.hasOption("h")) {
      usage(options);
      return;
    }

    try {
      // Now parse the required options
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.severe("Error parsing command line options: " + e.getMessage());
      usage(options);
      System.exit(1);
    }

    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    String topologyName = cmd.getOptionValue("topology_name");
    String topologyJarFile = cmd.getOptionValue("topology_jar");
    int schedulerServerPort = Integer.parseInt(cmd.getOptionValue("http_port"));

    // locate the topology definition file in the sandbox/working directory
    String topologyDefnFile = TopologyUtils.lookUpTopologyDefnFile(".", topologyName);

    // load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    // build the config by expanding all the variables
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultConfigs())
            .putAll(commandLineConfigs(cluster, role, environ))
            .putAll(topologyConfigs(topologyJarFile, topologyDefnFile, topology))
            .build());

    // Set up logging with complete Config
    setupLogging(config);

    LOG.log(Level.INFO, "Loaded scheduler config: {0}", config);

    // run the scheduler
    runScheduler(config, schedulerServerPort, topology);
  }

  // Set up logging basing on the Config
  public static void setupLogging(Config config) throws IOException {
    String systemConfigFilename = Context.systemConfigSandboxFile(config);

    SystemConfig systemConfig = new SystemConfig(systemConfigFilename, true);

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.INFO;
    // TODO(mfu): The folder creation may be duplicated with heron-executor in future
    // TODO(mfu): Remove the creation in future if feasible
    String loggingDir = systemConfig.getHeronLoggingDirectory();
    if (!FileUtils.isDirectoryExists(loggingDir)) {
      FileUtils.createDirectory(loggingDir);
    }

    // Log to file
    LoggingHelper.loggerInit(loggingLevel, true);
    // TODO(mfu): Pass the scheduler id from cmd
    String processId = String.format("%s-%s-%s", "heron", Context.topologyName(config), "scheduler");
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(processId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSizeMb() * Constants.MB_TO_BYTES,
            systemConfig.getHeronLoggingMaximumFiles()));

    LOG.info("Logging setup done.");
  }

  public static void runScheduler(
      Config config, int schedulerServerPort, TopologyAPI.Topology topology) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = (IStateManager) Class.forName(statemgrClass).newInstance();

    // create an instance of the packing class
    String packingClass = Context.packingClass(config);
    IPacking packing = (IPacking) Class.forName(packingClass).newInstance();

    // build the runtime config
    Config runtime = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinition(), topology)
        .put(Keys.schedulerStateManagerAdaptor(), new SchedulerStateManagerAdaptor(statemgr))
        .put(Keys.numContainers(), 1 + TopologyUtils.getNumContainers(topology))
        .build();

    SchedulerServer server = null;

    // Put it in a try block so that we can always clean resources
    try {
      // initialize the state manager
      statemgr.initialize(config);

      // get a packed plan and schedule it
      packing.initialize(config, runtime);
      PackingPlan packedPlan = packing.pack();

      // TODO - investigate whether the heron executors can be started
      // in scheduler.schedule method - rather than in scheduler.initialize method
      Config ytruntime = Config.newBuilder()
          .putAll(runtime)
          .put(Keys.instanceDistribution(), TopologyUtils.packingToString(packedPlan))
          .put(Keys.schedulerShutdown(), new Shutdown())
          .build();

      // create an instance of scheduler
      String schedulerClass = Context.schedulerClass(config);
      IScheduler scheduler = (IScheduler) Class.forName(schedulerClass).newInstance();

      // initialize the scheduler
      scheduler.initialize(config, ytruntime);

      // start the scheduler REST endpoint for receiving requests
      server = runServer(ytruntime, scheduler, schedulerServerPort);

      // write the scheduler location to state manager.
      setSchedulerLocation(runtime, server);

      // schedule the packed plan
      scheduler.schedule(packedPlan);

      // wait until kill request or some interrupt occurs
      LOG.info("Waiting for termination... ");
      Runtime.schedulerShutdown(ytruntime).await();
    } catch (Exception e) {
      // Log and exit the process
      LOG.log(Level.SEVERE, "Exception occurred", e);
      LOG.log(Level.SEVERE, "Failed to run scheduler for topology: {0}. Exiting...", topology.getName());
      System.exit(1);
    } finally {
      // Clean the resources
      if (server != null) {
        server.stop();
      }
      statemgr.close();
    }

    // stop the server and close the state manager
    LOG.log(Level.INFO, "Shutting down topology: {0}", topology.getName());

    System.exit(0);
  }

  /**
   * Run the http server for receiving scheduler requests
   *
   * @param runtime, the runtime configuration
   * @param scheduler, an instance of the scheduler
   * @param port, the port for scheduler to listen on
   * @return an instance of the http server
   */
  public static SchedulerServer runServer(
      Config runtime, IScheduler scheduler, int port) throws IOException {

    // create an instance of the server using scheduler class and port
    final SchedulerServer schedulerServer = new SchedulerServer(runtime, scheduler, port);

    // start the http server to manage runtime requests
    schedulerServer.start();

    return schedulerServer;
  }

  /**
   * Set the location of scheduler for other processes to discover
   *
   * @param runtime, the runtime configuration
   * @param schedulerServer, the http server that scheduler listens for receives requests
   */
  public static void setSchedulerLocation(Config runtime, SchedulerServer schedulerServer) {

    // Set scheduler location to host:port by default. Overwrite scheduler location if behind DNS.
    Scheduler.SchedulerLocation location = Scheduler.SchedulerLocation.newBuilder()
        .setTopologyName(Runtime.topologyName(runtime))
        .setHttpEndpoint(String.format("%s:%d", schedulerServer.getHost(), schedulerServer.getPort()))
        .build();

    LOG.log(Level.INFO, "Setting SchedulerLocation: {0}", location);
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    statemgr.setSchedulerLocation(location, Runtime.topologyName(runtime));
    // TODO - Should we wait on the future here
  }
}
