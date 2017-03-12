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

package com.twitter.heron.statemgr;

import javax.naming.OperationNotSupportedException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.Lock;
import com.twitter.heron.spi.statemgr.WatchCallback;

public class NullStateManager implements IStateManager {
  public SettableFuture<Boolean> nullFuture = SettableFuture.create();

  @Override
  public void initialize(Config config) {
    nullFuture.set(null);
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
  public ListenableFuture<Boolean> setTMasterLocation(
      TopologyMaster.TMasterLocation location,
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
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(
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
}
