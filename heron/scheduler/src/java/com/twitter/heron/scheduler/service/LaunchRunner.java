package com.twitter.heron.scheduler.service;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.api.ILauncher;
import com.twitter.heron.scheduler.api.IPackingAlgorithm;
import com.twitter.heron.scheduler.api.PackingPlan;
import com.twitter.heron.scheduler.api.SchedulerStateManagerAdaptor;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.util.NetworkUtility;

/**
 * Runs Launcher and launch topology. Also Uploads launch state to state manager.
 */
public class LaunchRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(LaunchRunner.class.getName());
  private ILauncher launcher;
  private LaunchContext context;
  private IPackingAlgorithm packingAlgorithm;
  private TopologyAPI.Topology topology;

  public LaunchRunner(ILauncher launcher,
                      LaunchContext context,
                      IPackingAlgorithm packingAlgorithm) {
    this.topology = context.getTopology();
    this.launcher = launcher;
    this.context = context;
    this.packingAlgorithm = packingAlgorithm;
  }

  public ExecutionEnvironment.ExecutionState createBasicExecutionState() {
    ExecutionEnvironment.ExecutionState executionState;
    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder();
    builder.setTopologyName(topology.getName());
    builder.setTopologyId(topology.getId());
    builder.setSubmissionTime(System.currentTimeMillis() / 1000);
    builder.setSubmissionUser(System.getProperty("user.name"));
    if (builder.isInitialized()) {
      executionState = builder.build();
    } else {
      return null;
    }
    return executionState;
  }

  public TopologyAPI.Topology trimTopology(TopologyAPI.Topology topology) {
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
    for (TopologyAPI.Spout.Builder spout : builder.getSpoutsBuilderList()) {
      spout.getCompBuilder().clearJavaObject();
    }
    for (TopologyAPI.Bolt.Builder bolt : builder.getBoltsBuilderList()) {
      bolt.getCompBuilder().clearJavaObject();
    }
    return builder.build();
  }

  public PackingPlan generatePacking() {
    // Set values in packing.
    return packingAlgorithm.pack(context);
  }

  public Boolean call() {
    launcher.initialize(context);

    PackingPlan packing = generatePacking();

    // Invoke the prepare for launching
    if (!launcher.prepareLaunch(packing)) {
      LOG.severe(launcher.getClass().getName() +
          " Failed to prepare launch topology locally.");
      return false;
    }

    SchedulerStateManagerAdaptor stateManager = context.getStateManagerAdaptor();
    ExecutionEnvironment.ExecutionState executionState =
        launcher.updateExecutionState(createBasicExecutionState());

    if (!NetworkUtility.awaitResult(
        stateManager.setExecutionState(executionState),
        5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to set execution state");
      return false;
    }
    if (!NetworkUtility.awaitResult(
        stateManager.setTopology(trimTopology(topology)),
        5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to set topology");
      stateManager.clearExecutionState();
      return false;
    }

    try {
      if (!launcher.launchTopology(packing)) {
        throw new RuntimeException(launcher.getClass().getName() + " Failed ");
      }
    } catch (RuntimeException e) {
      stateManager.clearExecutionState();
      stateManager.clearTopology();
      LOG.log(Level.SEVERE, "Failed to launch topology remotely", e);
      return false;
    }

    // Invoke the post for launching
    if (!launcher.postLaunch(packing)) {
      LOG.severe(launcher.getClass().getName() +
          " Failed to post launch topology locally.");
      return false;
    }

    return true;
  }
}
