package com.twitter.heron.state.localfile;

import java.util.Map;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.core.base.FileUtility;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.state.FileSystemStateManager;
import com.twitter.heron.state.WatchCallback;

public class LocalFileStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(LocalFileStateManager.class.getName());

  public static final String IS_INITIALIZE_FILE_TREE = "is.initialize.file.tree";
  public static final String WORKING_DIRECTORY = "heron.local.working.directory";

  @Override
  public void initialize(Map<Object, Object> conf) {
    String rootAddress = (String) conf.get(ROOT_ADDRESS);
    if (rootAddress == null) {
      Object workingDir = conf.get(WORKING_DIRECTORY);
      if (workingDir == null) {
        throw new IllegalArgumentException("Misses required config: " + WORKING_DIRECTORY);
      }
      rootAddress = String.format("%s/%s", workingDir, "state");
      conf.put(ROOT_ADDRESS, rootAddress);
    }

    super.initialize(conf);

    // By default, we would init the file tree if it is not there
    boolean isInitLocalFileTree = conf.get(IS_INITIALIZE_FILE_TREE) == null ?
        true : (Boolean) conf.get(IS_INITIALIZE_FILE_TREE);

    if (isInitLocalFileTree && !initTree()) {
      throw new IllegalArgumentException("Failed to initialize Local State manager. " +
          "Check rootAddress: " + rootAddress);
    }
  }

  protected boolean initTree() {
    // Make necessary directories
    if (FileUtility.isDirectoryExists(getTopologyDir()) || FileUtility.createDirectory(getTopologyDir()) &&
        FileUtility.isDirectoryExists(getTMasterLocationDir()) || FileUtility.createDirectory(getTMasterLocationDir()) &&
        FileUtility.isDirectoryExists(getPhysicalPlanDir()) || FileUtility.createDirectory(getPhysicalPlanDir()) &&
        FileUtility.isDirectoryExists(getExecutionStateDir()) || FileUtility.createDirectory(getExecutionStateDir()) &&
        FileUtility.isDirectoryExists(getSchedulerLocationDir()) || FileUtility.createDirectory(getSchedulerLocationDir())) {
      return true;
    }

    return false;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.writeToFile(getExecutionStatePath(topologyName),
        executionState.toByteArray());
    future.set(ret);

    return future;
  }


  @Override
  public ListenableFuture<Boolean> setTMasterLocation(TopologyMaster.TMasterLocation location, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.writeToFile(getTMasterLocationPath(topologyName),
        location.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.writeToFile(getTopologyPath(topologyName), topology.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.writeToFile(getPhysicalPlanPath(topologyName), physicalPlan.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.writeToFile(getSchedulerLocationPath(topologyName),
        location.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.deleteFile(getTMasterLocationPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.deleteFile(getSchedulerLocationPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.deleteFile(getExecutionStatePath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.deleteFile(getTopologyPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.deleteFile(getPhysicalPlanPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(WatchCallback watcher, String topologyName) {
    SettableFuture<Scheduler.SchedulerLocation> future = SettableFuture.create();
    String path = getSchedulerLocationPath(topologyName);
    byte[] data = FileUtility.readFromFile(path);
    Scheduler.SchedulerLocation location;
    try {
      location = Scheduler.SchedulerLocation.parseFrom(data);
      future.set(location);
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse SchedulerLocation", e));
    }

    return future;
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(WatchCallback watcher, String topologyName) {
    SettableFuture<TopologyAPI.Topology> future = SettableFuture.create();
    String path = getTopologyPath(topologyName);
    byte[] data = FileUtility.readFromFile(path);
    TopologyAPI.Topology topology;
    try {
      topology = TopologyAPI.Topology.parseFrom(data);
      future.set(topology);
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse SchedulerLocation", e));
    }

    return future;
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(WatchCallback watcher, String topologyName) {
    SettableFuture<ExecutionEnvironment.ExecutionState> future = SettableFuture.create();
    String path = getExecutionStatePath(topologyName);
    byte[] data = FileUtility.readFromFile(path);
    ExecutionEnvironment.ExecutionState executionState;
    try {
      executionState = ExecutionEnvironment.ExecutionState.parseFrom(data);
      future.set(executionState);
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse SchedulerLocation", e));
    }

    return future;
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(WatchCallback watcher, String topologyName) {
    SettableFuture<PhysicalPlans.PhysicalPlan> future = SettableFuture.create();
    String path = getPhysicalPlanPath(topologyName);
    byte[] data = FileUtility.readFromFile(path);
    PhysicalPlans.PhysicalPlan physicalPlan;
    try {
      physicalPlan = PhysicalPlans.PhysicalPlan.parseFrom(data);
      future.set(physicalPlan);
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse SchedulerLocation", e));
    }

    return future;
  }

  @Override
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(WatchCallback watcher, String topologyName) {
    SettableFuture<TopologyMaster.TMasterLocation> future = SettableFuture.create();
    String path = getTMasterLocationPath(topologyName);
    byte[] data = FileUtility.readFromFile(path);
    TopologyMaster.TMasterLocation location;
    try {
      location = TopologyMaster.TMasterLocation.parseFrom(data);
      future.set(location);
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse TMasterLocation", e));
    }

    return future;
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtility.isFileExists(getTopologyPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public void close() {
    // We would not clear anything here
    // Scheduler kill interface should take care of the cleaning
  }
}
