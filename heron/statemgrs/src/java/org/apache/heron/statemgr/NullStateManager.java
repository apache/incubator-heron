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

package org.apache.heron.statemgr;

import java.util.logging.Logger;

import javax.naming.OperationNotSupportedException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.statemgr.Lock;
import org.apache.heron.spi.statemgr.WatchCallback;

public class NullStateManager implements IStateManager {

  private static final Logger LOG = Logger.getLogger(NullStateManager.class.getName());

  public final SettableFuture<Boolean> nullFuture = SettableFuture.create();

  @Override
  public void initialize(Config config) {
    if (!nullFuture.set(null)) {
      LOG.warning("Unexpected - NullStateManager is initialized twice!");
    }
  }

  @Override
  public void close() {

  }

  @Override
  public Lock getLock(String topologyName, LockName lockName) {
    throw new RuntimeException(new OperationNotSupportedException());
  }

  @Override
  public ListenableFuture<Boolean> deleteLocks(String topologyName) {
    throw new RuntimeException(new OperationNotSupportedException());
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    return null;
  }

  @Override
  public ListenableFuture<Boolean> setTManagerLocation(
      TopologyManager.TManagerLocation location,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setMetricsCacheLocation(
      TopologyManager.MetricsCacheLocation location,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setTopology(
      TopologyAPI.Topology topology,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setPackingPlan(
      PackingPlans.PackingPlan packingPlan,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteTManagerLocation(String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<Boolean> deleteMetricsCacheLocation(String topologyName) {
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
  public ListenableFuture<Boolean> deletePackingPlan(String topologyName) {
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
  public ListenableFuture<TopologyManager.TManagerLocation> getTManagerLocation(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<TopologyManager.MetricsCacheLocation> getMetricsCacheLocation(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<PackingPlans.PackingPlan> getPackingPlan(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<Boolean> setStatefulCheckpoints(
      CheckpointManager.StatefulConsistentCheckpoints checkpoint,
      String topologyName) {
    return nullFuture;
  }

  @Override
  public ListenableFuture<CheckpointManager.StatefulConsistentCheckpoints> getStatefulCheckpoints(
      WatchCallback watcher,
      String topologyName) {
    return SettableFuture.create();
  }

  @Override
  public ListenableFuture<Boolean> deleteStatefulCheckpoints(String topologyName) {
    return nullFuture;
  }
}
