package com.twitter.heron.spi.statemgr;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;

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
  public void initialize(Config config);

  public void close();

  /**
   * Is the given topology in RUNNING state?
   * @param topologyName
   * @return Boolean
   */
  ListenableFuture<Boolean> isTopologyRunning(String topologyName);

  /**
   * Set the execution state for the given topology
   * @param executionState
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName);

  /**
   * Set the topology definition for the given topology
   * @param topology
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setTopology(
      TopologyAPI.Topology topology, String topologyName);

  /**
   * Set the scheduler location for the given top
   * @param location
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location, String topologyName);

  /**
   * Delete the tmaster location for the given topology
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteTMasterLocation(String topologyName);

  /**
   * Delete the execution state for the given topology
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteExecutionState(String topologyName);

  /**
   * Delete the topology definition for the given topology
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteTopology(String topologyName);

  /**
   * Delete the physical plan for the given topology
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deletePhysicalPlan(String topologyName);

  /**
   * Delete the scheduler location for the given topology
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName);

  /**
   * Get the tmaster location for the given topology
   * @param watcher @see com.twitter.heron.spi.statemgr.WatchCallback
   * @param topologyName
   * @return TMasterLocation
   */
  ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(
      WatchCallback watcher, String topologyName);

  /**
   * Get the scheduler location for the given topology
   * @param watcher @see com.twitter.heron.spi.statemgr.WatchCallback
   * @param topologyName
   * @return SchedulerLocation
   */
  ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName);

  /**
   * Get the topology definition for the given topology
   * @param watcher @see com.twitter.heron.spi.statemgr.WatchCallback
   * @param topologyName
   * @return Topology
   */
  ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName);

  /**
   * Get the execution state for the given topology
   * @param watcher @see com.twitter.heron.spi.statemgr.WatchCallback
   * @param topologyName
   * @return ExecutionState
   */
  ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName);

  /**
   * Set the location of Tmaster.
   * @param location
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setTMasterLocation(
      TopologyMaster.TMasterLocation location, String topologyName);

  /**
   * Set the physical plan for the given topology
   * @param physicalPlan
   * @param topologyName
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan, String topologyName);

  /**
   * Get the physical plan for the given topology
   * @param watcher @see com.twitter.heron.spi.statemgr.WatchCallback
   * @param topologyName
   * @return PhysicalPlan
   */
  ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName);
}
