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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.scheduler.client.ISchedulerClient;
import org.apache.heron.scheduler.client.SchedulerClientFactory;
import org.apache.heron.scheduler.dryrun.SubmitDryRunResponse;
import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.PackingPlanProtoSerializer;
import org.apache.heron.spi.scheduler.ILauncher;
import org.apache.heron.spi.scheduler.LauncherException;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;

/**
 * Runs Launcher and launch topology. Also Uploads launch state to state manager.
 */
public class LaunchRunner {
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
        .setSubmissionUser(Context.submitUser(config))
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

  private PackingPlans.PackingPlan createPackingPlan(PackingPlan packingPlan) {
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(packingPlan);
  }

  /**
   * Call launcher to launch topology
   *
   * @throws LauncherException
   * @throws PackingException
   * @throws SubmitDryRunResponse
   */
  public void call() throws LauncherException, PackingException, SubmitDryRunResponse {
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    String topologyName = Context.topologyName(config);

    PackingPlan packedPlan = LauncherUtils.getInstance().createPackingPlan(config, runtime);

    if (Context.dryRun(config)) {
      throw new SubmitDryRunResponse(topology, config, packedPlan);
    }

    // initialize the launcher
    launcher.initialize(config, runtime);

    // Set topology def first since we determine whether a topology is running
    // by checking the existence of topology def
    // store the trimmed topology definition into the state manager
    // TODO(rli): log-and-false anti-pattern is too nested on this path. will not refactor
    Boolean result = statemgr.setTopology(trimTopology(topology), topologyName);
    if (result == null || !result) {
      throw new LauncherException(String.format(
          "Failed to set topology definition for topology '%s'", topologyName));
    }

    result = statemgr.setPackingPlan(createPackingPlan(packedPlan), topologyName);
    if (result == null || !result) {
      statemgr.deleteTopology(topologyName);
      throw new LauncherException(String.format(
          "Failed to set packing plan for topology '%s'", topologyName));
    }

    // store the execution state into the state manager
    ExecutionEnvironment.ExecutionState executionState = createExecutionState();

    result = statemgr.setExecutionState(executionState, topologyName);
    if (result == null || !result) {
      statemgr.deletePackingPlan(topologyName);
      statemgr.deleteTopology(topologyName);
      throw new LauncherException(String.format(
          "Failed to set execution state for topology '%s'", topologyName));
    }

    // Launch the topology, clear the state if it fails. Some schedulers throw exceptions instead of
    // returning false. In some cases the scheduler needs to have the topology deleted.
    try {
      if (!launcher.launch(packedPlan)) {
        throw new TopologySubmissionException(null);
      }
    } catch (TopologySubmissionException e) {
      // Compile error message to throw.
      final StringBuilder errorMessage = new StringBuilder(
          String.format("Failed to launch topology '%s'", topologyName));
      if (e.getMessage() != null) {
        errorMessage.append("\n").append(e.getMessage());
      }

      try {
        // Clear state from the Scheduler via RPC.
        Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest
            .newBuilder()
            .setTopologyName(topologyName).build();

        ISchedulerClient schedulerClient = new SchedulerClientFactory(config, runtime)
            .getSchedulerClient();
        if (!schedulerClient.killTopology(killTopologyRequest)) {
          final String logMessage =
              String.format("Failed to remove topology '%s' from scheduler after failed submit. "
                  + "Please re-try the kill command.", topologyName);
          errorMessage.append("\n").append(logMessage);
          LOG.log(Level.SEVERE, logMessage);
        }
      // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Exception ignored){
        // The above call to clear the Scheduler may fail. This situation can be ignored.
        LOG.log(Level.FINE,
            String.format("Failure clearing failed topology `%s` from Scheduler during `submit`",
                topologyName));
      }

      // Clear state from the State Manager.
      statemgr.deleteExecutionState(topologyName);
      statemgr.deletePackingPlan(topologyName);
      statemgr.deleteTopology(topologyName);
      throw new LauncherException(errorMessage.toString());
    }
  }
}
