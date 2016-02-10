package com.twitter.heron.spi.scheduler;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.state.IStateManager;

/**
 * This file provides a Adaptor for Scheduler.
 * It provides only the methods needed for Scheduler,
 * and provides easier interfaces to invoke in Scheduler.
 */

public class SchedulerStateManagerAdaptor {
  private final IStateManager delegate;
  private final String topologyName;

  /**
   * Construct SchedulerStateManagerAdaptor, providing only interfaces used by Scheduler.
   *
   * @param delegate the IStateManager already initialized. Noticed that the initialize and close of
   * IStateManager is not in the SchedulerStateManagerAdaptor. And users would have no ways to
   * make it since SchedulerStateManagerAdaptor does not provide those interfaces.
   * @param topologyName The name of topology
   */
  public SchedulerStateManagerAdaptor(IStateManager delegate, String topologyName) {
    this.delegate = delegate;
    this.topologyName = topologyName;
  }

  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState) {
    return delegate.setExecutionState(executionState, topologyName);
  }

  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology) {
    return delegate.setTopology(topology, topologyName);
  }

  public ListenableFuture<Boolean> setSchedulerLocation(Scheduler.SchedulerLocation location) {
    return delegate.setSchedulerLocation(location, topologyName);
  }

  public ListenableFuture<Boolean> clearTMasterLocation() {
    return delegate.deleteTMasterLocation(topologyName);
  }

  public ListenableFuture<Boolean> clearExecutionState() {
    return delegate.deleteExecutionState(topologyName);
  }

  public ListenableFuture<Boolean> clearTopology() {
    return delegate.deleteTopology(topologyName);
  }

  public ListenableFuture<Boolean> clearPhysicalPlan() {
    return delegate.deletePhysicalPlan(topologyName);
  }

  public ListenableFuture<Boolean> clearSchedulerLocation() {
    return delegate.deleteSchedulerLocation(topologyName);
  }

  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation() {
    return delegate.getTMasterLocation(null, topologyName);
  }

  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation() {
    return delegate.getSchedulerLocation(null, topologyName);
  }

  public ListenableFuture<TopologyAPI.Topology> getTopology() {
    return delegate.getTopology(null, topologyName);
  }

  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState() {
    return delegate.getExecutionState(null, topologyName);
  }

  public ListenableFuture<Boolean> isTopologyRunning() {
    return delegate.isTopologyRunning(topologyName);
  }
}
