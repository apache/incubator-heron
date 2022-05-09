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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.proto.tmanager.TopologyManager;

/**
 * This file provides a Adaptor for Scheduler.
 * It provides only the methods needed for Scheduler,
 * and provides easier interfaces to invoke in Scheduler.
 */

public class SchedulerStateManagerAdaptor {
  private static final Logger LOG = Logger.getLogger(SchedulerStateManagerAdaptor.class.getName());

  private final IStateManager delegate;
  private final int timeout;

  /**
   * Construct SchedulerStateManagerAdaptor providing only the
   * interfaces used by scheduler.
   *
   * @param delegate an instance of IStateManager that is already initialized.
   * Noticed that the initialize and close of IStateManager is not in the
   * SchedulerStateManager. Users are restricted from using those interfaces
   * since it is upto the abstract scheduler to decide when to open and close.
   *
   * @param timeout the maximum time to wait in milliseconds
   */
  public SchedulerStateManagerAdaptor(IStateManager delegate, int timeout) {
    this.delegate = delegate;
    this.timeout = timeout;
  }

  /**
   * Waits for ListenableFuture to terminate. Cancels on timeout
   */
  protected <V> V awaitResult(ListenableFuture<V> future) {
    return awaitResult(future, timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits for ListenableFuture to terminate. Cancels on timeout
   */
  protected <V> V awaitResult(ListenableFuture<V> future, int time, TimeUnit unit) {
    try {
      return future.get(time, unit);
    } catch (ExecutionException e) {
      LOG.log(Level.WARNING, "Exception processing future: " + e.getMessage());
      future.cancel(true);
      return null;
    } catch (InterruptedException | TimeoutException e) {
      LOG.log(Level.SEVERE, "Exception processing future ", e);
      future.cancel(true);
      return null;
    }
  }

  public Lock getLock(String topologyName, IStateManager.LockName lockName) {
    return delegate.getLock(topologyName, lockName);
  }

  /**
   * Is the given topology in RUNNING state?
   *
   * @return Boolean
   */
  public Boolean isTopologyRunning(String topologyName) {
    return awaitResult(delegate.isTopologyRunning(topologyName));
  }

  /**
   * Set the execution state for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    return awaitResult(delegate.setExecutionState(executionState, topologyName));
  }

  /**
   * Set the topology definition for the given topology
   *
   * @param topologyName the name of the topology
   * @return Boolean - Success or Failure
   */
  public Boolean setTopology(TopologyAPI.Topology topology, String topologyName) {
    return awaitResult(delegate.setTopology(topology, topologyName));
  }

  /**
   * Update the topology definition for the given topology. If the topology doesn't exist,
   * create it. If it does, update it.
   *
   * @param topologyName the name of the topology
   * @return Boolean - Success or Failure
   */
  public Boolean updateTopology(TopologyAPI.Topology topology, String topologyName) {
    if (getTopology(topologyName) != null) {
      deleteTopology(topologyName);
    }
    return setTopology(topology, topologyName);
  }

  /**
   * Set the scheduler location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean setSchedulerLocation(
      Scheduler.SchedulerLocation location,
      String topologyName) {
    return awaitResult(delegate.setSchedulerLocation(location, topologyName));
  }

  /**
   * Set the packing plan for the given topology
   *
   * @param packingPlan the packing plan of the topology
   * @return Boolean - Success or Failure
   */
  public Boolean setPackingPlan(PackingPlans.PackingPlan packingPlan, String topologyName) {
    return awaitResult(delegate.setPackingPlan(packingPlan, topologyName));
  }

  /**
   * Update the packing plan for the given topology. If the packing plan doesn't exist, create it.
   * If it does, update it.
   *
   * @param packingPlan the packing plan of the topology
   * @return Boolean - Success or Failure
   */
  public Boolean updatePackingPlan(PackingPlans.PackingPlan packingPlan, String topologyName) {
    if (getPackingPlan(topologyName) != null) {
      deletePackingPlan(topologyName);
    }
    return setPackingPlan(packingPlan, topologyName);
  }

  /**
   * Delete the tmanager location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deleteTManagerLocation(String topologyName) {
    return awaitResult(delegate.deleteTManagerLocation(topologyName));
  }

  /**
   * Delete the metricscache location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deleteMetricsCacheLocation(String topologyName) {
    return awaitResult(delegate.deleteMetricsCacheLocation(topologyName));
  }

  /**
   * Delete the execution state for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deleteExecutionState(String topologyName) {
    return awaitResult(delegate.deleteExecutionState(topologyName));
  }

  /**
   * Delete the topology definition for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deleteTopology(String topologyName) {
    return awaitResult(delegate.deleteTopology(topologyName));
  }

  /**
   * Delete the packing plan for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deletePackingPlan(String topologyName) {
    return awaitResult(delegate.deletePackingPlan(topologyName));
  }

  /**
   * Delete the physical plan for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deletePhysicalPlan(String topologyName) {
    return awaitResult(delegate.deletePhysicalPlan(topologyName));
  }

  /**
   * Delete the scheduler location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deleteSchedulerLocation(String topologyName) {
    return awaitResult(delegate.deleteSchedulerLocation(topologyName));
  }

  public Boolean deleteLocks(String topologyName) {
    return awaitResult(delegate.deleteLocks(topologyName));
  }

  public Boolean deleteStatefulCheckpoint(String topologyName) {
    return awaitResult(delegate.deleteStatefulCheckpoints(topologyName));
  }

  /**
   * Get the tmanager location for the given topology
   *
   * @return TManagerLocation
   */
  public TopologyManager.TManagerLocation getTManagerLocation(String topologyName) {
    return awaitResult(delegate.getTManagerLocation(null, topologyName));
  }

  /**
   * Get the scheduler location for the given topology
   *
   * @return SchedulerLocation
   */
  public Scheduler.SchedulerLocation getSchedulerLocation(String topologyName) {
    return awaitResult(delegate.getSchedulerLocation(null, topologyName));
  }

  /**
   * Get the metricscache location for the given topology
   *
   * @return MetricsCacheLocation
   */
  public TopologyManager.MetricsCacheLocation getMetricsCacheLocation(String topologyName) {
    return awaitResult(delegate.getMetricsCacheLocation(null, topologyName));
  }


  /**
   * Get the topology definition for the given topology
   *
   * @return Topology
   */
  public TopologyAPI.Topology getTopology(String topologyName) {
    return awaitResult(delegate.getTopology(null, topologyName));
  }

  /**
   * Get the execution state for the given topology
   *
   * @return ExecutionState
   */
  public ExecutionEnvironment.ExecutionState getExecutionState(String topologyName) {
    return awaitResult(delegate.getExecutionState(null, topologyName));
  }

  /**
   * Get the physical plan for the given topology
   *
   * @return PhysicalPlans.PhysicalPlan
   */
  public PhysicalPlans.PhysicalPlan getPhysicalPlan(String topologyName) {
    return awaitResult(delegate.getPhysicalPlan(null, topologyName));
  }

  /**
   * Get the packing plan for the given topology
   *
   * @return PackingPlans.PackingPlan
   */
  public PackingPlans.PackingPlan getPackingPlan(String topologyName) {
    return awaitResult(delegate.getPackingPlan(null, topologyName));
  }
}
