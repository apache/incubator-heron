package com.twitter.heron.statemgr.localfs;

import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.statemgr.WatchCallback;
import com.twitter.heron.statemgr.FileSystemStateManager;

public class LocalFileSystemStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemStateManager.class.getName());

  private Config config;

  @Override
  public void initialize(Config config) {

    super.initialize(config);
    this.config = config;

    // By default, we would init the file tree if it is not there
    boolean isInitLocalFileTree = LocalFileSystemContext.initLocalFileTree(config);

    if (isInitLocalFileTree && !initTree()) {
      throw new IllegalArgumentException("Failed to initialize Local State manager. " +
          "Check rootAddress: " + rootAddress);
    }
  }

  protected boolean initTree() {
    // Make necessary directories
    LOG.info("Topologies directory: " + getTopologyDir()); 
    LOG.info("Tmaster location directory: " + getTMasterLocationDir());
    LOG.info("Physical plan directory: " + getPhysicalPlanDir()); 
    LOG.info("Execution state directory: " + getExecutionStateDir());
    LOG.info("Scheduler location directory: " + getSchedulerLocationDir());

    if ((FileUtils.isDirectoryExists(getTopologyDir()) || FileUtils.createDirectory(getTopologyDir())) &&
        (FileUtils.isDirectoryExists(getTMasterLocationDir()) || FileUtils.createDirectory(getTMasterLocationDir())) &&
        (FileUtils.isDirectoryExists(getPhysicalPlanDir()) || FileUtils.createDirectory(getPhysicalPlanDir())) &&
        (FileUtils.isDirectoryExists(getExecutionStateDir()) || FileUtils.createDirectory(getExecutionStateDir())) &&
        (FileUtils.isDirectoryExists(getSchedulerLocationDir()) || FileUtils.createDirectory(getSchedulerLocationDir()))) {
      return true;
    }

    return false;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(getExecutionStatePath(topologyName),
        executionState.toByteArray());
    future.set(ret);

    return future;
  }


  @Override
  public ListenableFuture<Boolean> setTMasterLocation(TopologyMaster.TMasterLocation location, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(getTMasterLocationPath(topologyName),
        location.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(getTopologyPath(topologyName), topology.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(getPhysicalPlanPath(topologyName), physicalPlan.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(getSchedulerLocationPath(topologyName),
        location.toByteArray());
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.deleteFile(getTMasterLocationPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.deleteFile(getSchedulerLocationPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.deleteFile(getExecutionStatePath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.deleteFile(getTopologyPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.deleteFile(getPhysicalPlanPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(WatchCallback watcher, String topologyName) {
    SettableFuture<Scheduler.SchedulerLocation> future = SettableFuture.create();
    String path = getSchedulerLocationPath(topologyName);
    byte[] data = FileUtils.readFromFile(path);
    if(data.length == 0){
      future.set(null);
      return future;
    }

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
    byte[] data = FileUtils.readFromFile(path);
    if(data.length == 0){
      future.set(null);
      return future;
    }

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
    byte[] data = FileUtils.readFromFile(path);
    if(data.length == 0){
      future.set(null);
      return future;
    }

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
    byte[] data = FileUtils.readFromFile(path);
    if(data.length == 0){
      future.set(null);
      return future;
    }

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
    byte[] data = FileUtils.readFromFile(path);
    if(data.length == 0){
      future.set(null);
      return future;
    }

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
    boolean ret = FileUtils.isFileExists(getTopologyPath(topologyName));
    future.set(ret);

    return future;
  }

  @Override
  public void close() {
    // We would not clear anything here
    // Scheduler kill interface should take care of the cleaning
  }
}
