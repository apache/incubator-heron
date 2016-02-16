package com.twitter.heron.statemgr;

import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.WatchCallback;

@SuppressWarnings("unchecked")
public class NullStateManager implements IStateManager {
  public SettableFuture nullFuture = SettableFuture.create();

  @Override
  public void initialize(Context context) {
    nullFuture.set(null);
  }

  @Override
  public Context getContext() {
    return null;
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
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(ExecutionEnvironment.ExecutionState executionState, String
      topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(WatchCallback watcher, String
      topologyName) {

    return nullFuture;
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(WatchCallback watcher, String
      topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(WatchCallback watcher, String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(WatchCallback watcher, String
      topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(WatchCallback watcher, String topologyName) {
    return nullFuture;
  }
}
