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

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.Slf4jUtils;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.logging.LoggingHelper;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.scheduler.server.SchedulerServer;
import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerConfigUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.Shutdown;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.PackingPlanProtoDeserializer;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.ReflectionUtils;

/**
 * Main class of scheduler.
 */
public class SchedulerMain {
  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  private int schedulerServerPort = -1;       // http port where the scheduler is listening

  private TopologyAPI.Topology topology = null;  // topology definition
  private Config config;                        // holds all the config read
  // Properties passed from command line property arguments
  private final Properties properties;

  public SchedulerMain(
      Config config,
      TopologyAPI.Topology topology,
      int schedulerServerPort,
      Properties properties) {
    // initialize the options
    this.config = config;
    this.topology = topology;
    this.schedulerServerPort = schedulerServerPort;
    this.properties = properties;
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
        .desc("Topology jar/pex file path")
        .longOpt("topology_bin")
        .hasArgs()
        .argName("topology binary file")
        .required()
        .build();

    Option schedulerHTTPPort = Option.builder("p")
        .desc("Http Port number on which the scheduler listens for requests")
        .longOpt("http_port")
        .hasArgs()
        .argName("http port")
        .required()
        .build();

    Option property = Option.builder(
        SchedulerUtils.SCHEDULER_COMMAND_LINE_PROPERTIES_OVERRIDE_OPTION)
        .desc("use value for given property")
        .longOpt("property_override")
        .hasArgs()
        .valueSeparator()
        .argName("property=value")
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(topologyName);
    options.addOption(topologyJar);
    options.addOption(schedulerHTTPPort);
    options.addOption(property);
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
    Slf4jUtils.installSLF4JBridge();
    // construct the options and help options first.
    Options options = constructOptions();
    Options helpOptions = constructHelpOptions();

    // parse the options
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(helpOptions, args, true);

    // print help, if we receive wrong set of arguments
    if (cmd.hasOption("h")) {
      usage(options);
      return;
    }

    // Now parse the required options
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      usage(options);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    // It returns a new empty Properties instead of null,
    // if no properties passed from command line. So no need for null check.
    Properties schedulerProperties =
        cmd.getOptionProperties(SchedulerUtils.SCHEDULER_COMMAND_LINE_PROPERTIES_OVERRIDE_OPTION);

    // initialize the scheduler with the options
    String topologyName = cmd.getOptionValue("topology_name");
    SchedulerMain schedulerMain = createInstance(cmd.getOptionValue("cluster"),
        cmd.getOptionValue("role"),
        cmd.getOptionValue("environment"),
        cmd.getOptionValue("topology_bin"),
        topologyName,
        Integer.parseInt(cmd.getOptionValue("http_port")),
        cmd.hasOption("verbose"),
        schedulerProperties);

    LOG.info("Scheduler command line properties override: " + schedulerProperties.toString());

    // run the scheduler
    boolean ret = schedulerMain.runScheduler();

    // Log the result and exit
    if (!ret) {
      throw new RuntimeException("Failed to schedule topology: " + topologyName);
    } else {
      // stop the server and close the state manager
      LOG.log(Level.INFO, "Shutting down topology: {0}", topologyName);
    }
  }

  public static SchedulerMain createInstance(String cluster,
                                             String role,
                                             String env,
                                             String topologyJar,
                                             String topologyName,
                                             int httpPort,
                                             Boolean verbose
                                             ) throws IOException, InvalidTopologyException {
    return createInstance(
        cluster, role, env, topologyJar, topologyName, httpPort, verbose, new Properties());
  }

  public static SchedulerMain createInstance(String cluster,
                                             String role,
                                             String env,
                                             String topologyJar,
                                             String topologyName,
                                             int httpPort,
                                             Boolean verbose,
                                             Properties schedulerProperties)
      throws IOException, InvalidTopologyException {
    // Look up the topology def file location
    String topologyDefnFile = TopologyUtils.lookUpTopologyDefnFile(".", topologyName);

    // load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    // build the config by expanding all the variables
    Config schedulerConfig = SchedulerConfigUtils.loadConfig(
        cluster,
        role,
        env,
        topologyJar,
        topologyDefnFile,
        verbose,
        topology);

    // set up logging with complete Config
    setupLogging(schedulerConfig);

    // Create a new instance
    SchedulerMain schedulerMain =
        new SchedulerMain(schedulerConfig, topology, httpPort, schedulerProperties);

    LOG.log(Level.INFO, "Loaded scheduler config: {0}", schedulerMain.config);
    return schedulerMain;
  }

  // Set up logging based on the Config
  private static void setupLogging(Config config) throws IOException {
    String systemConfigFilename = Context.systemConfigFile(config);

    SystemConfig systemConfig = SystemConfig.newBuilder(true)
        .putAll(systemConfigFilename, true)
        .build();

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.INFO;
    if (Context.verbose(config).booleanValue()) {
      loggingLevel = Level.FINE;
    }
    // TODO(mfu): The folder creation may be duplicated with heron-executor in future
    // TODO(mfu): Remove the creation in future if feasible
    String loggingDir = systemConfig.getHeronLoggingDirectory();
    if (!FileUtils.isDirectoryExists(loggingDir)) {
      FileUtils.createDirectory(loggingDir);
    }

    // Log to file
    LoggingHelper.loggerInit(loggingLevel, true);

    // TODO(mfu): Pass the scheduler id from cmd
    String processId =
        String.format("%s-%s-%s", "heron", Context.topologyName(config), "scheduler");
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(processId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSize(),
            systemConfig.getHeronLoggingMaximumFiles()));

    LOG.info("Logging setup done.");
  }

  /**
   * Get the http server for receiving scheduler requests
   *
   * @param runtime the runtime configuration
   * @param scheduler an instance of the scheduler
   * @param port the port for scheduler to listen on
   * @return an instance of the http server
   */
  protected SchedulerServer getServer(
      Config runtime, IScheduler scheduler, int port) throws IOException {

    // create an instance of the server using scheduler class and port
    return new SchedulerServer(runtime, scheduler, port);
  }

  /**
   * Run the scheduler.
   * It is a blocking call, and it will return in 2 cases:
   * 1. The topology is requested to kill
   * 2. Unexpected exceptions happen
   *
   * @return true if scheduled successfully
   */
  public boolean runScheduler() {
    IScheduler scheduler = null;

    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr;

    try {
      // create an instance of state manager
      statemgr = ReflectionUtils.newInstance(statemgrClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.log(Level.SEVERE, "Failed to instantiate instances using config: " + config, e);
      return false;
    }

    SchedulerServer server = null;
    boolean isSuccessful = false;

    // Put it in a try block so that we can always clean resources
    try {
      // initialize the state manager
      statemgr.initialize(config);

      // TODO(mfu): timeout should read from config
      SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);

      // get a packed plan and schedule it
      PackingPlans.PackingPlan serializedPackingPlan = adaptor.getPackingPlan(topology.getName());
      if (serializedPackingPlan == null) {
        LOG.log(Level.SEVERE, "Failed to fetch PackingPlan for topology:{0} from the state manager",
            topology.getName());
        return false;
      }
      LOG.log(Level.INFO, "Packing plan fetched from state: {0}", serializedPackingPlan);
      PackingPlan packedPlan = new PackingPlanProtoDeserializer().fromProto(serializedPackingPlan);

      // build the runtime config
      LauncherUtils launcherUtils = LauncherUtils.getInstance();
      Config runtime = Config.newBuilder()
          .putAll(launcherUtils.createPrimaryRuntime(topology))
          .putAll(launcherUtils.createAdaptorRuntime(adaptor))
          .put(Key.SCHEDULER_SHUTDOWN, getShutdown())
          .put(Key.SCHEDULER_PROPERTIES, properties)
          .build();

      Config ytruntime = launcherUtils.createConfigWithPackingDetails(runtime, packedPlan);

      // invoke scheduler
      scheduler = launcherUtils.getSchedulerInstance(config, ytruntime);
      if (scheduler == null) {
        return false;
      }

      isSuccessful = scheduler.onSchedule(packedPlan);
      if (!isSuccessful) {
        LOG.severe("Failed to schedule topology");
        return false;
      }

      // Failures in server initialization throw exceptions
      // get the scheduler server endpoint for receiving requests
      server = getServer(ytruntime, scheduler, schedulerServerPort);
      // start the server to manage runtime requests
      server.start();

      // write the scheduler location to state manager
      // Make sure it happens after IScheduler.onScheduler
      isSuccessful = SchedulerUtils.setSchedulerLocation(
          runtime, String.format("%s:%d", server.getHost(), server.getPort()), scheduler);

      if (isSuccessful) {
        // wait until kill request or some interrupt occurs if the scheduler starts successfully
        LOG.info("Waiting for termination... ");
        Runtime.schedulerShutdown(ytruntime).await();
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to start server", e);
      return false;
    } finally {
      // Clean the resources
      if (server != null) {
        server.stop();
      }

      // 4. Close the resources
      SysUtils.closeIgnoringExceptions(scheduler);
      SysUtils.closeIgnoringExceptions(statemgr);
    }

    return isSuccessful;
  }

  // Utils method
  protected Shutdown getShutdown() {
    return new Shutdown();
  }
}
