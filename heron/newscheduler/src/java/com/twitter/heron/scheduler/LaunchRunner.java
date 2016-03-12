package com.twitter.heron.scheduler;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;

/**
 * Runs Launcher and launch topology. Also Uploads launch state to state manager.
 */
public class LaunchRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(LaunchRunner.class.getName());

  private Config config;
  private Config runtime;

  private ILauncher launcher;
  private IPacking packing;

  public LaunchRunner(Config config, Config runtime) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    this.config = config;
    this.runtime = runtime;

    // Create an instance of the launcher class 
    String launcherClass = Context.launcherClass(config);
    this.launcher = (ILauncher)Class.forName(launcherClass).newInstance();

    // Create an instance of the packing class
    String packingClass = Context.packingClass(config);
    this.packing = (IPacking)Class.forName(packingClass).newInstance();
  }

  public ExecutionEnvironment.ExecutionState createBasicExecutionState() {
    ExecutionEnvironment.ExecutionState executionState;
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    ExecutionEnvironment.ExecutionState.Builder builder = 
        ExecutionEnvironment.ExecutionState.newBuilder();

    // set the topology name, id, submitting user and time
    builder.setTopologyName(topology.getName());
    builder.setTopologyId(topology.getId());
    builder.setSubmissionTime(System.currentTimeMillis() / 1000);
    builder.setSubmissionUser(System.getProperty("user.name"));

    if (!builder.isInitialized()) {
      return null;
    }
    executionState = builder.build();
    return executionState;
  }

  /**
   * Trim the topology definition for storing into state manager.
   * This is because the user generated spouts and bolts
   * might be huge. 
   *
   * @return trimmed topology
   */
  public TopologyAPI.Topology trimTopology(TopologyAPI.Topology topology) {

    // create a copy of the topology physical plan
    TopologyAPI.Topology.Builder builder = topology.newBuilder().mergeFrom(topology);

    // clear the state of user spout java objects - which can be potentially huge
    for (TopologyAPI.Spout.Builder spout : builder.getSpoutsBuilderList()) {
      spout.getCompBuilder().clearJavaObject();
    }

    // clear the state of user spout java objects - which can be potentially huge
    for (TopologyAPI.Bolt.Builder bolt : builder.getBoltsBuilderList()) {
      bolt.getCompBuilder().clearJavaObject();
    }

    return builder.build();
  }

  public Boolean call() {

    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    String topologyName = Context.topologyName(config);

    // get the packed plan
    packing.initialize(config, runtime);
    PackingPlan packedPlan = packing.pack();

    // initialize the launcher 
    launcher.initialize(config, runtime);

    // invoke the prepare launch sequence
    if (!launcher.prepareLaunch(packedPlan)) {
      LOG.severe(launcher.getClass().getName() + " Failed to prepare launch topology locally.");
      return false;
    }

    // store the execution state into the state manager
    ExecutionEnvironment.ExecutionState executionState =
        launcher.updateExecutionState(createBasicExecutionState());

    ListenableFuture<Boolean> sFuture = statemgr.setExecutionState(executionState, topologyName);
    if (!NetworkUtils.awaitResult(sFuture, 5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to set execution state");
      return false;
    }

    // store the trimmed topology definition into the state manager
    ListenableFuture<Boolean> tFuture = statemgr.setTopology(trimTopology(topology), topologyName);
    if (!NetworkUtils.awaitResult(tFuture, 5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to set topology");
      statemgr.deleteExecutionState(topologyName);
      return false;
    }

    // launch the topology, clear the state if it fails
    try {
      if (!launcher.launch(packedPlan)) {
        throw new RuntimeException(launcher.getClass().getName() + " failed ");
      }
    } catch (RuntimeException e) {
      statemgr.deleteExecutionState(topologyName);
      statemgr.deleteTopology(topologyName);
      LOG.log(Level.SEVERE, "Failed to launch topology remotely", e);
      return false;
    }

    // invoke the post launch sequence 
    if (!launcher.postLaunch(packedPlan)) {
      LOG.severe(launcher.getClass().getName() + " failed to post launch topology locally.");
      return false;
    }

    return true;
  }
}
