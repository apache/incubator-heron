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
    this.launcher = Runtime.launcherClassInstance(runtime);
    this.packing = Runtime.packingClassInstance(runtime);
  }

  public ExecutionEnvironment.ExecutionState createExecutionState() {
    // TODO(mfu): These values should read from config
    String releaseUsername = "heron";
    String releaseTag = "heron-core";
    String releaseVersion = "live";

    TopologyAPI.Topology topology = Runtime.topology(runtime);

    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder();

    // set the topology name, id, submitting user and time
    builder.setTopologyName(topology.getName()).
        setTopologyId(topology.getId())
        .setSubmissionTime(System.currentTimeMillis() / 1000)
        .setSubmissionUser(System.getProperty("user.name"))
        .setCluster(Context.cluster(config))
        .setRole(Context.role(config))
        .setEnviron(Context.environ(config));

    // build the heron release state
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();

    releaseBuilder.setReleaseUsername(releaseUsername);
    releaseBuilder.setReleaseTag(releaseTag);
    releaseBuilder.setReleaseVersion(releaseVersion);

    builder.setReleaseState(releaseBuilder);
    if (builder.isInitialized()) {
      return builder.build();
    } else {
      throw new RuntimeException("Failed to create execution state");
    }
  }

  public Boolean call() {
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    String topologyName = Context.topologyName(config);

    // get the packed plan
    packing.initialize(config, runtime);
    PackingPlan packedPlan = packing.pack();

    // initialize the launcher 
    launcher.initialize(config, runtime);

    // invoke the prepare launch sequence
    if (!launcher.prepareLaunch(packedPlan)) {
      LOG.log(Level.SEVERE,
          "{0} failed to prepare launch topology locally",
          launcher.getClass().getName());
      return false;
    }

    // store the execution state into the state manager
    ExecutionEnvironment.ExecutionState executionState = createExecutionState();

    ListenableFuture<Boolean> sFuture = statemgr.setExecutionState(executionState, topologyName);
    if (!NetworkUtils.awaitResult(sFuture, 5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to set execution state");
      return false;
    }

    // launch the topology, clear the state if it fails
    try {
      if (!launcher.launch(packedPlan)) {
        throw new RuntimeException(launcher.getClass().getName() + " failed ");
      }
    } catch (RuntimeException e) {
      statemgr.deleteExecutionState(topologyName);
      LOG.log(Level.SEVERE, "Failed to launch topology remotely", e);
      return false;
    }

    // invoke the post launch sequence 
    if (!launcher.postLaunch(packedPlan)) {
      LOG.log(Level.SEVERE,
          "{0} failed to post launch topology locally",
          launcher.getClass().getName());
      return false;
    }

    return true;
  }
}
