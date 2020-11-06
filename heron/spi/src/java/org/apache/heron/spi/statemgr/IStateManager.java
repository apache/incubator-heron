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

package org.apache.heron.spi.statemgr;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.classification.InterfaceAudience;
import org.apache.heron.classification.InterfaceStability;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.common.Config;

/**
 * This file defines the IStateManager interface.
 * <p>
 * Services across Heron use HeronStateMgr to get/set state information.
 * Currently the primary things kept by state are:
 * 1. Where is the the topology manager running.
 * The topology manager is responsible for writing this information out
 * upon startup. The stream managers query this upon startup to find out
 * who is their topology manager. In case they loose connection with
 * the topology manager, the stream managers query this again to see
 * if the topology manager has changed.
 * <p>
 * 2. Topology and the current running state of the topology
 * This information is seeded by the topology submitter.
 * The topology manager updates this when the state of the topology
 * changes.
 * <p>
 * 3. Current assignment.
 * This information is solely used by topology manager. When it
 * creates a new assignment or when the assignment changes, it writes
 * out this information. This is required for topology manager failover.
 * <p>
 * Clients call the methods of the state passing a callback. The callback
 * is called with result code upon the completion of the operation.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public interface IStateManager extends AutoCloseable {
  enum LockName {
    UPDATE_TOPOLOGY("updateTopology");

    private String name;

    LockName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * Initialize StateManager with the incoming context.
   */
  void initialize(Config config);

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the StateManager
   * <p>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();

  /**
   * Is the given topology in RUNNING state?
   *
   * @return Boolean
   */
  ListenableFuture<Boolean> isTopologyRunning(String topologyName);

  /**
   * Return a lock object backed by the state manager store.
   *
   * @param topologyName the name of the topology
   * @param lockName any thread may get the {@code Lock} object bound to a given name, but only one
   * thread may obtain the actual lock from that @{code Lock} object.
   * @return an object representing an implementation of a lock.
   */
  Lock getLock(String topologyName, LockName lockName);

  /**
   * Delete all locks for a given topology. Ideally locks should be deleted when released but it's
   * possible that some state systems (e.g., ZooKeeper) will not delete all resources when a lock is
   * released. This method should be invoked to clean all such lock resources.
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteLocks(String topologyName);

  /**
   * Set the location of Tmanager.
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setTManagerLocation(
      TopologyManager.TManagerLocation location, String topologyName);

  /**
   * Get the tmanager location for the given topology
   *
   * @param watcher @see org.apache.heron.spi.statemgr.WatchCallback
   * @return TManagerLocation
   */
  ListenableFuture<TopologyManager.TManagerLocation> getTManagerLocation(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the tmanager location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteTManagerLocation(String topologyName);

  /**
   * Set the location of MetricsCache.
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setMetricsCacheLocation(
      TopologyManager.MetricsCacheLocation location, String topologyName);

  /**
   * Get the MetricsCache location for the given topology
   *
   * @param watcher @see org.apache.heron.spi.statemgr.WatchCallback
   * @return TManagerLocation
   */
  ListenableFuture<TopologyManager.MetricsCacheLocation> getMetricsCacheLocation(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the metricscache location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteMetricsCacheLocation(String topologyName);

  /**
   * Set the topology definition for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setTopology(
      TopologyAPI.Topology topology, String topologyName);

  /**
   * Get the topology definition for the given topology
   *
   * @param watcher @see org.apache.heron.spi.statemgr.WatchCallback
   * @return Topology
   */
  ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the topology definition for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteTopology(String topologyName);

  /**
   * Set the packing plan for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setPackingPlan(
      PackingPlans.PackingPlan packingPlan, String topologyName);

  /**
   * Get the packing plan for the given topology
   *
   * @param watcher @see org.apache.heron.spi.statemgr.WatchCallback
   * @return PackingPlan
   */
  ListenableFuture<PackingPlans.PackingPlan> getPackingPlan(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the packing plan for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deletePackingPlan(String topologyName);

  /**
   * Set the physical plan for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan, String topologyName);

  /**
   * Get the physical plan for the given topology
   *
   * @param watcher @see org.apache.heron.spi.statemgr.WatchCallback
   * @return PhysicalPlan
   */
  ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the physical plan for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deletePhysicalPlan(String topologyName);

  /**
   * Set the execution state for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName);

  /**
   * Get the execution state for the given topology
   *
   * @param watcher @see org.apache.heron.spi.statemgr.WatchCallback
   * @return ExecutionState
   */
  ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the execution state for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteExecutionState(String topologyName);

  /**
   * Set the scheduler location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location, String topologyName);

  /**
   * Get the scheduler location for the given topology
   *
   * @param watcher @see org.apache.heron.spi.statemgr.WatchCallback
   * @return SchedulerLocation
   */
  ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the scheduler location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName);

  /**
   * Set the Stateful Checkpoints
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> setStatefulCheckpoints(
      CheckpointManager.StatefulConsistentCheckpoints checkpoint, String topologyName);

  /**
   * Get the Stateful Checkpoints
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<CheckpointManager.StatefulConsistentCheckpoints> getStatefulCheckpoints(
      WatchCallback watcher, String topologyName);

  /**
   * Delete the stateful checkpoints for the given topology
   *
   * @return Boolean - Success or Failure
   */
  ListenableFuture<Boolean> deleteStatefulCheckpoints(String topologyName);
}
