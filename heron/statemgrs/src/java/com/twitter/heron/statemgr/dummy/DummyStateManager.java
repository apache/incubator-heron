package com.twitter.heron.statemgr.dummy;

import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.WatchCallback;

public class DummyStateManager implements IStateManager {
  public SettableFuture<Boolean> brightFuture = SettableFuture.create();

  @Override
  public void initialize(Map<Object, Object> conf) {
    brightFuture.set(true);
  }

  @Override
  public void close() {

  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    return null;
  }

  @Override
  public ListenableFuture<Boolean> setTMasterLocation(TopologyMaster.TMasterLocation location, String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    return brightFuture;
  }

  @Override
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(WatchCallback watcher, String topologyName) {
    return null;
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(WatchCallback watcher, String topologyName) {
    return null;
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(WatchCallback watcher, String topologyName) {
    return null;
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(WatchCallback watcher, String topologyName) {
    return null;
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(WatchCallback watcher, String topologyName) {
    return null;
  }
}
