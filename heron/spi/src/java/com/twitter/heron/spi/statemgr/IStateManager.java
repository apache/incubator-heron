package com.twitter.heron.spi.statemgr;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Context;

/**
 * This file defines the IStateManager interface.
 * <p/>
 * Services across Heron use HeronStateMgr to get/set state information.
 * Currently the primary things kept by state are:
 * 1. Where is the the topology master running.
 * The topology master is responsible for writing this information out
 * upon startup. The stream managers query this upon startup to find out
 * who is their topology master. In case they loose connection with
 * the topology master, the stream managers query this again to see
 * if the topology master has changed.
 * <p/>
 * 2. Topology and the current running state of the topology
 * This information is seeded by the topology submitter.
 * The topology master updates this when the state of the topology
 * changes.
 * <p/>
 * 3. Current assignment.
 * This information is solely used by topology master. When it
 * creates a new assignment or when the assignment changes, it writes
 * out this information. This is required for topology master failover.
 * <p/>
 * Clients call the methods of the state passing a callback. The callback
 * is called with result code upon the completion of the operation.
 */

public interface IStateManager {

  /**
   * Initialize the uploader with the incoming context.
   */
  public void initialize(Context context);

  /**
   * Get the context specific to the StateManager
   *
   * @return Context
   */
  public Context getContext();

  void close();

  ListenableFuture<Boolean> isTopologyRunning(String topologyName);

  ListenableFuture<Boolean> setTMasterLocation(
      TopologyMaster.TMasterLocation location, String topologyName);

  ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName);

  ListenableFuture<Boolean> setTopology(
      TopologyAPI.Topology topology, String topologyName);

  ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan, String topologyName);

  ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location, String topologyName);

  ListenableFuture<Boolean> deleteTMasterLocation(String topologyName);

  ListenableFuture<Boolean> deleteExecutionState(String topologyName);

  ListenableFuture<Boolean> deleteTopology(String topologyName);

  ListenableFuture<Boolean> deletePhysicalPlan(String topologyName);

  ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName);

  ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(
      WatchCallback watcher, String topologyName);

  ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName);

  ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName);

  ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName);

  ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName);
}
