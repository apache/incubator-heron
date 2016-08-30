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

package com.twitter.heron.spi.statemgr;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;

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
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Exception processing future ", e);
      future.cancel(true);
      return null;
    }
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
   * Delete the tmaster location for the given topology
   *
   * @return Boolean - Success or Failure
   */
  public Boolean deleteTMasterLocation(String topologyName) {
    return awaitResult(delegate.deleteTMasterLocation(topologyName));
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

  /**
   * Get the tmaster location for the given topology
   *
   * @return TMasterLocation
   */
  public TopologyMaster.TMasterLocation getTMasterLocation(String topologyName) {
    return awaitResult(delegate.getTMasterLocation(null, topologyName));
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
   * Get the topology definition for the given topology
   *
   * @return Topology
   */
  public TopologyAPI.Topology getTopology(String topologyName) {
    return awaitResult(delegate.getTopology(null, topologyName));
  }

  /**
   * Checks to see if the execution state exists
   *
   * @return a boolean indicating whether the execution state exists or not
   */
  public boolean doesExecutionStateExist(String topologyName) {
    return awaitResult(delegate.executionStateExists(topologyName));
  }

  /**
   * Clean all states of a heron topology. This goes through each piece of state that needs
   * to be cleaned up and will log out a warning if it could not be cleaned up properly.
   * TMasterLocation, PackingPlan, PhysicalPlan, SchedulerLocation, ExecutionState, and Topology
   * @param topologyName the name of the topology that we should clean the state for
   */
  public void cleanState(String topologyName) {
    LOG.fine("Cleaning up topology state");

    logDeleteStatus(deleteTMasterLocation(topologyName), "topology master");
    logDeleteStatus(deletePackingPlan(topologyName), "packing plan");
    logDeleteStatus(deletePhysicalPlan(topologyName), "physical plan");
    logDeleteStatus(deleteSchedulerLocation(topologyName), "scheduler");
    logDeleteStatus(deleteExecutionState(topologyName), "execution state");
    logDeleteStatus(deleteTopology(topologyName), "topology definition");

    LOG.fine("Cleaned up topology state");
  }

  private void logDeleteStatus(Boolean success, String name) {
    if (success == null || !success) {
      LOG.warning("Failed to clear state for " + name);
    }
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
