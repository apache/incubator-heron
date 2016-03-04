package com.twitter.heron.scheduler;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;

import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.IStateManager;

import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;

import com.twitter.heron.scheduler.server.SchedulerServer;

/**
 * Main class of scheduler.
 */
public class SchedulerMain {
  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  /**
   * To be launched with topology name (args[0]), scheduler class (args[1]).
   * args[2] will be config loader class and args[3] will be config overrides.
   * and optionally scheduler config file (args[4]).
   */
  public static void main(String[] args) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    String cluster = args[0];
    String role = args[1];
    String environ = args[2];
    String topologyName = args[3];
    int schedulerServerPort = Integer.parseInt(args[4]);

    // first load the defaults, then the config from files to override it
    Config.Builder defaultConfigs = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterConfig.loadSandboxConfig());

    // add config parameters from the command line
    Config.Builder commandLineConfigs = Config.newBuilder()
        .put(Keys.get("CLUSTER"), cluster)
        .put(Keys.get("ROLE"), role)
        .put(Keys.get("ENVIRON"), environ);

    // locate the topology definition file in the sandbox/working directory
    String topologyDefnFile = TopologyUtils.lookUpTopologyDefnFile(".", topologyName);

    // load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    Config.Builder topologyConfigs = Config.newBuilder()
        .put(Keys.get("TOPOLOGY_ID"), topology.getId())
        .put(Keys.get("TOPOLOGY_NAME"), topology.getName());
    
    Config config = Config.newBuilder()
        .putAll(defaultConfigs.build())
        .putAll(commandLineConfigs.build())
        .putAll(topologyConfigs.build())
        .build(); 

    // run the scheduler
    runScheduler(config, schedulerServerPort, topology);
  }

  public static void runScheduler(Config config, int schedulerServerPort, TopologyAPI.Topology topology) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = (IStateManager)Class.forName(statemgrClass).newInstance();

    // build the runtime config
    Config runtime = Config.newBuilder()
        .put(Keys.get("TOPOLOGY_ID"), topology.getId())
        .put(Keys.get("TOPOLOGY_NAME"), topology.getName())
        .put(Keys.get("TOPOLOGY_DEFINITION"), topology)
        .put(Keys.get("STATE_MANAGER"), statemgr)
        .build();

    // create an instance of scheduler 
    String schedulerClass = Context.schedulerClass(config);
    IScheduler scheduler = (IScheduler)Class.forName(schedulerClass).newInstance();

    // initialize the scheduler
    scheduler.initialize(config, runtime);

    // start the scheduler REST endpoint for receiving requests
    SchedulerServer server = runServer(runtime, scheduler, schedulerServerPort);

    // write the scheduler location to state manager.
    setSchedulerLocation(runtime, server);

    // create an instance of the packing class 
    String packingClass = Context.packingClass(config);
    IPacking packing = (IPacking)Class.forName(packingClass).newInstance();

    // get a packed plan and schedule it
    packing.initialize(config, runtime);
    PackingPlan packedPlan = packing.pack();

    scheduler.schedule(packedPlan);

    // We close the state manager when we receive a Kill Request
    // It is handled in KillRequest Handler
  }

  /**
   * Run the http server for receiving scheduler requests
   */ 
  public static SchedulerServer runServer(Config runtime, IScheduler scheduler, int port)
      throws IOException {

    // create an instance of the server using scheduler class and port
    final SchedulerServer schedulerServer = new SchedulerServer(runtime, scheduler, port);

    // start the http server
    schedulerServer.start();

    return schedulerServer;
  }

  public static void setSchedulerLocation(Config runtime, SchedulerServer schedulerServer) {

    // Set scheduler location to host:port by default. Overwrite scheduler location if behind DNS.
    Scheduler.SchedulerLocation location = Scheduler.SchedulerLocation.newBuilder()
        .setTopologyName(Runtime.topologyName(runtime))
        .setHttpEndpoint(String.format("%s:%d", schedulerServer.getHost(), schedulerServer.getPort()))
        .build();

    LOG.info("Setting SchedulerLocation: " + location);
    IStateManager statemgr = Runtime.stateManager(runtime);
    statemgr.setSchedulerLocation(location, Runtime.topologyName(runtime));
    // TODO - Should we wait on the future here
  }
}
