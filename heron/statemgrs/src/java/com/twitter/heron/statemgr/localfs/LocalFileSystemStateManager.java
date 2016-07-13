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

package com.twitter.heron.statemgr.localfs;

import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.WatchCallback;
import com.twitter.heron.statemgr.FileSystemStateManager;

public class LocalFileSystemStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemStateManager.class.getName());

  @Override
  public void initialize(Config ipconfig) {

    super.initialize(ipconfig);

    // By default, we would init the file tree if it is not there
    boolean isInitLocalFileTree = LocalFileSystemContext.initLocalFileTree(ipconfig);

    if (isInitLocalFileTree && !initTree()) {
      throw new IllegalArgumentException("Failed to initialize Local State manager. "
          + "Check rootAddress: " + rootAddress);
    }
  }

  protected boolean initTree() {
    // Make necessary directories
    LOG.log(Level.FINE, "Topologies directory: {0}", getTopologyDir());
    LOG.log(Level.FINE, "Tmaster location directory: {0}", getTMasterLocationDir());
    LOG.log(Level.FINE, "Physical plan directory: {0}", getPhysicalPlanDir());
    LOG.log(Level.FINE, "Execution state directory: {0}", getExecutionStateDir());
    LOG.log(Level.FINE, "Scheduler location directory: {0}", getSchedulerLocationDir());

    boolean topologyDir = FileUtils.isDirectoryExists(getTopologyDir())
        || FileUtils.createDirectory(getTopologyDir());

    boolean tmasterLocationDir = FileUtils.isDirectoryExists(getTMasterLocationDir())
        || FileUtils.createDirectory(getTMasterLocationDir());

    boolean physicalPlanDir = FileUtils.isDirectoryExists(getPhysicalPlanDir())
        || FileUtils.createDirectory(getPhysicalPlanDir());

    boolean executionStateDir = FileUtils.isDirectoryExists(getExecutionStateDir())
        || FileUtils.createDirectory(getExecutionStateDir());

    boolean schedulerLocationDir = FileUtils.isDirectoryExists(getSchedulerLocationDir())
        || FileUtils.createDirectory(getSchedulerLocationDir());

    return topologyDir && tmasterLocationDir && physicalPlanDir && executionStateDir
        && schedulerLocationDir;
  }

  // Make utils class protected for easy unit testing
  protected ListenableFuture<Boolean> setData(String path, byte[] data, boolean overwrite) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(path, data, overwrite);
    future.set(ret);

    return future;
  }

  protected ListenableFuture<Boolean> deleteData(String path) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.deleteFile(path);
    future.set(ret);

    return future;
  }

  @SuppressWarnings("unchecked") // we don't know what M is until runtime
  protected <M extends Message> ListenableFuture<M> getData(String path, Message.Builder builder) {
    final SettableFuture<M> future = SettableFuture.create();
    byte[] data = FileUtils.readFromFile(path);
    if (data.length == 0) {
      future.set(null);
      return future;
    }

    try {
      builder.mergeFrom(data);
      future.set((M) builder.build());
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse " + Message.Builder.class, e));
    }

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    return setData(getExecutionStatePath(topologyName), executionState.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setTMasterLocation(
      TopologyMaster.TMasterLocation location, String topologyName) {
    // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
    // This is because when running in simulator we control when a tmaster dies and
    // comes up deterministically.
    return setData(getTMasterLocationPath(topologyName), location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    return setData(getTopologyPath(topologyName), topology.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    return setData(getPhysicalPlanPath(topologyName), physicalPlan.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location, String topologyName) {
    // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
    // This is because when running in simulator we control when a scheduler dies and
    // comes up deterministically.
    return setData(getSchedulerLocationPath(topologyName), location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    return deleteData(getTMasterLocationPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    return deleteData(getSchedulerLocationPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    return deleteData(getExecutionStatePath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    return deleteData(getTopologyPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    return deleteData(getPhysicalPlanPath(topologyName));
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName) {
    return getData(getSchedulerLocationPath(topologyName),
        Scheduler.SchedulerLocation.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName) {
    return getData(getTopologyPath(topologyName), TopologyAPI.Topology.newBuilder());
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName) {
    return getData(getExecutionStatePath(topologyName),
        ExecutionEnvironment.ExecutionState.newBuilder());
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName) {
    return getData(getPhysicalPlanPath(topologyName),
        PhysicalPlans.PhysicalPlan.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(
      WatchCallback watcher, String topologyName) {
    return getData(getTMasterLocationPath(topologyName),
        TopologyMaster.TMasterLocation.newBuilder());
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.isFileExists(getTopologyPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public void close() {
    // We would not clear anything here
    // Scheduler kill interface should take care of the cleaning
  }

  /**
   * Returns all information stored in the StateManager. This is a utility method used for debugging
   * while developing. To invoke, run:
   *
   *   bazel run heron/statemgrs/src/java:localfs-statemgr-unshaded -- &lt;topology-name&gt;
   */
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    if (args.length < 1) {
      throw new RuntimeException(String.format(
          "Usage: java %s <topology_name> - view state manager details for a topology",
          LocalFileSystemStateManager.class.getCanonicalName()));
    }

    String topologyName = args[0];
    Config config = Config.newBuilder()
        .put(Keys.stateManagerRootPath(),
            System.getProperty("user.home") + "/.herondata/repository/state/local")
        .build();

    print("==> State Manager root path: %s", config.get(Keys.stateManagerRootPath()));

    com.twitter.heron.spi.statemgr.IStateManager stateManager = new LocalFileSystemStateManager();
    stateManager.initialize(config);

    if (stateManager.isTopologyRunning(topologyName).get()) {
      print("==> Topology %s found", topologyName);
      print("==> ExecutionState:\n%s",
          stateManager.getExecutionState(null, topologyName).get());
      print("==> SchedulerLocation:\n%s",
          stateManager.getSchedulerLocation(null, topologyName).get());
      print("==> TMasterLocation:\n%s",
          stateManager.getTMasterLocation(null, topologyName).get());
      print("==> PhysicalPlan:\n%s",
          stateManager.getPhysicalPlan(null, topologyName).get());
    } else {
      print("==> Topology %s not found under %s",
          topologyName, config.get(Keys.stateManagerRootPath()));
    }
  }

  private static void print(String format, Object... values) {
    System.out.println(String.format(format, values));
  }
}
