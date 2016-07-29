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

package com.twitter.heron.statemgr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.WatchCallback;

public abstract class FileSystemStateManager implements IStateManager {
  private static final Logger LOG = Logger.getLogger(FileSystemStateManager.class.getName());

  // Store the root address of the hierarchical file system
  protected String rootAddress;

  protected abstract ListenableFuture<Boolean> nodeExists(String path);

  protected abstract ListenableFuture<Boolean> deleteNode(String path);

  protected abstract <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                                         String path,
                                                                         Message.Builder builder);

  protected String getTMasterLocationDir() {
    return concatPath(rootAddress, "tmasters");
  }

  protected String getTopologyDir() {
    return concatPath(rootAddress, "topologies");
  }

  protected String getPackingPlanDir() {
    return concatPath(rootAddress, "packingplans");
  }

  protected String getPhysicalPlanDir() {
    return concatPath(rootAddress, "pplans");
  }

  protected String getExecutionStateDir() {
    return concatPath(rootAddress, "executionstate");
  }

  protected String getSchedulerLocationDir() {
    return concatPath(rootAddress, "schedulers");
  }

  protected String getTMasterLocationPath(String topologyName) {
    return concatPath(getTMasterLocationDir(), topologyName);
  }

  protected String getTopologyPath(String topologyName) {
    return concatPath(getTopologyDir(), topologyName);
  }

  protected String getPackingPlanPath(String topologyName) {
    return concatPath(getPackingPlanDir(), topologyName);
  }

  protected String getPhysicalPlanPath(String topologyName) {
    return concatPath(getPhysicalPlanDir(), topologyName);
  }

  protected String getExecutionStatePath(String topologyName) {
    return concatPath(getExecutionStateDir(), topologyName);
  }

  protected String getSchedulerLocationPath(String topologyName) {
    return concatPath(getSchedulerLocationDir(), topologyName);
  }

  @Override
  public void initialize(Config config) {
    this.rootAddress = Context.stateManagerRootPath(config);
    LOG.log(Level.FINE, "File system state manager root address: {0}", rootAddress);
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    return deleteNode(getTMasterLocationPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    return deleteNode(getSchedulerLocationPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    return deleteNode(getExecutionStatePath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    return deleteNode(getTopologyPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deletePackingPlan(String topologyName) {
    return deleteNode(getPackingPlanPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    return deleteNode(getPhysicalPlanPath(topologyName));
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getSchedulerLocationPath(topologyName),
        Scheduler.SchedulerLocation.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getTopologyPath(topologyName), TopologyAPI.Topology.newBuilder());
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getExecutionStatePath(topologyName),
        ExecutionEnvironment.ExecutionState.newBuilder());
  }

  @Override
  public ListenableFuture<PackingPlans.PackingPlan> getPackingPlan(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getPackingPlanPath(topologyName),
        PackingPlans.PackingPlan.newBuilder());
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getPhysicalPlanPath(topologyName),
        PhysicalPlans.PhysicalPlan.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getTMasterLocationPath(topologyName),
        TopologyMaster.TMasterLocation.newBuilder());
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    return nodeExists(getTopologyPath(topologyName));
  }

  private static String concatPath(String basePath, String appendPath) {
    return String.format("%s/%s", basePath, appendPath);
  }

  /**
   * Returns all information stored in the StateManager. This is a utility method used for debugging
   * while developing. To invoke, run:
   *
   *  bazel run heron/statemgrs/src/java:localfs-statemgr-unshaded -- \
   *    &lt;topology-name&gt; [new_instance_distribution]
   *
   * If a new_instance_distribution is provided, the instance distribution will be updated to
   * trigger a scaling event. For example:
   *
   *  bazel run heron/statemgrs/src/java:localfs-statemgr-unshaded -- \
   *    ExclamationTopology 1:word:3:0:exclaim1:2:0:exclaim1:1:0
   *
   */
  protected void doMain(String[] args, Config config)
      throws ExecutionException, InterruptedException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {

    if (args.length < 1) {
      throw new RuntimeException(String.format(
          "Usage: java %s <topology_name> - view state manager details for a topology",
          this.getClass().getCanonicalName()));
    }

    String topologyName = args[0];
    print("==> State Manager root path: %s", config.get(Keys.stateManagerRootPath()));

    initialize(config);

    PackingPlans.PackingPlan existingPackingPlan = null;
    if (isTopologyRunning(topologyName).get()) {
      print("==> Topology %s found", topologyName);
      print("==> ExecutionState:\n%s", getExecutionState(null, topologyName).get());
      print("==> SchedulerLocation:\n%s",
          getSchedulerLocation(null, topologyName).get());
      print("==> TMasterLocation:\n%s", getTMasterLocation(null, topologyName).get());
      existingPackingPlan = getPackingPlan(null, topologyName).get();
      print("==> PackingPlan:\n%s", existingPackingPlan);
      print("==> PhysicalPlan:\n%s", getPhysicalPlan(null, topologyName).get());
    } else {
      print("==> Topology %s not found under %s",
          topologyName, config.get(Keys.stateManagerRootPath()));
    }
  }

  protected void print(String format, Object... values) {
    System.out.println(String.format(format, values));
  }
}
