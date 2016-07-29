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

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.LauncherUtils;
import com.twitter.heron.spi.utils.Runtime;

/**
 * Runs Launcher and launch topology. Also Uploads launch state to state manager.
 */
public class LaunchRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(LaunchRunner.class.getName());

  private Config config;
  private Config runtime;

  private ILauncher launcher;

  public LaunchRunner(Config config, Config runtime) {

    this.config = config;
    this.runtime = runtime;
    this.launcher = Runtime.launcherClassInstance(runtime);
  }

  public ExecutionEnvironment.ExecutionState createExecutionState() {
    String releaseUsername = Context.buildUser(config);
    // TODO(mfu): Currently we leave release tag empty
    String releaseTag = "";
    String releaseVersion = Context.buildVersion(config);

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

  /**
   * Trim the topology definition for storing into state manager.
   * This is because the user generated spouts and bolts
   * might be huge.
   *
   * @return trimmed topology
   */
  public TopologyAPI.Topology trimTopology(TopologyAPI.Topology topology) {

    // create a copy of the topology physical plan
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);

    // clear the state of user spout java objects - which can be potentially huge
    for (TopologyAPI.Spout.Builder spout : builder.getSpoutsBuilderList()) {
      spout.getCompBuilder().clearSerializedObject();
    }

    // clear the state of user spout java objects - which can be potentially huge
    for (TopologyAPI.Bolt.Builder bolt : builder.getBoltsBuilderList()) {
      bolt.getCompBuilder().clearSerializedObject();
    }

    return builder.build();
  }

  @Override
  public Boolean call() {
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    String topologyName = Context.topologyName(config);

    PackingPlan packedPlan = LauncherUtils.instance.createPackingPlan(config, runtime);
    if (packedPlan == null) {
      LOG.severe("Failed to pack a valid PackingPlan. Check the config.");
      return false;
    }

    // initialize the launcher
    launcher.initialize(config, runtime);

    Boolean result;

    // Set topology def first since we determine whether a topology is running
    // by checking the existence of topology def
    // store the trimmed topology definition into the state manager
    result = statemgr.setTopology(trimTopology(topology), topologyName);
    if (result == null || !result) {
      LOG.severe("Failed to set topology definition");
      return false;
    }

    // store the execution state into the state manager
    ExecutionEnvironment.ExecutionState executionState = createExecutionState();

    result = statemgr.setExecutionState(executionState, topologyName);
    if (result == null || !result) {
      LOG.severe("Failed to set execution state");
      statemgr.deleteTopology(topologyName);
      return false;
    }

    // launch the topology, clear the state if it fails
    if (!launcher.launch(packedPlan)) {
      statemgr.deleteExecutionState(topologyName);
      statemgr.deleteTopology(topologyName);
      LOG.log(Level.SEVERE, "Failed to launch topology");
      return false;
    }

    return true;
  }

}
