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

import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.statemgr.Lock;
import org.apache.heron.spi.statemgr.WatchCallback;

public abstract class FileSystemStateManager implements IStateManager {
  private static final Logger LOG = Logger.getLogger(FileSystemStateManager.class.getName());

  protected static <V> void safeSetFuture(SettableFuture<V> future, V result) {
    if (!future.set(result)) {
      LOG.warning("Unexpected - a local settable future is set twice!");
    }
  }

  protected static <V> void safeSetException(SettableFuture<V> future, Throwable cause) {
    if (!future.setException(cause)) {
      LOG.warning("Unexpected - a local settable future is set twice!");
    }
  }

  // Store the root address of the hierarchical file system
  protected String rootAddress;

  protected enum StateLocation {
    TMANAGER_LOCATION("tmanagers", "TManager location"),
    METRICSCACHE_LOCATION("metricscaches", "MetricsCache location"),
    TOPOLOGY("topologies", "Topologies"),
    PACKING_PLAN("packingplans", "Packing plan"),
    PHYSICAL_PLAN("pplans", "Physical plan"),
    EXECUTION_STATE("executionstate", "Execution state"),
    SCHEDULER_LOCATION("schedulers", "Scheduler location"),
    STATEFUL_CHECKPOINT("statefulcheckpoints", "Stateful checkpoints"),
    LOCKS("locks", "Distributed locks");

    private final String dir;
    private final String name;

    StateLocation(String dir, String name) {
      this.dir = dir;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public String getDirectory(String root) {
      return concatPath(root, dir);
    }

    public String getNodePath(String root, String topology) {
      return concatPath(getDirectory(root), topology);
    }

    public String getNodePath(String root, String topology, String extraToken) {
      return getNodePath(root, String.format("%s__%s", topology, extraToken));
    }

    private static String concatPath(String basePath, String appendPath) {
      return String.format("%s/%s", basePath, appendPath);
    }
  }

  protected abstract ListenableFuture<Boolean> nodeExists(String path);

  protected abstract ListenableFuture<Boolean> deleteNode(String path,
                                                          boolean deleteChildrenIfNecessary);

  protected abstract <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                                         String path,
                                                                         Message.Builder builder);

  protected abstract Lock getLock(String path);

  protected String getStateDirectory(StateLocation location) {
    return location.getDirectory(rootAddress);
  }

  protected String getStatePath(StateLocation location, String topologyName) {
    return location.getNodePath(rootAddress, topologyName);
  }

  @Override
  public void initialize(Config config) {
    this.rootAddress = Context.stateManagerRootPath(config);
    LOG.log(Level.FINE, "File system state manager root address: {0}", rootAddress);
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    return nodeExists(getStatePath(StateLocation.TOPOLOGY, topologyName));
  }

  @Override
  public Lock getLock(String topologyName, LockName lockName) {
    return getLock(
        StateLocation.LOCKS.getNodePath(this.rootAddress, topologyName, lockName.getName()));
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.SCHEDULER_LOCATION, topologyName,
        Scheduler.SchedulerLocation.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.TOPOLOGY, topologyName,
        TopologyAPI.Topology.newBuilder());
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.EXECUTION_STATE, topologyName,
        ExecutionEnvironment.ExecutionState.newBuilder());
  }

  @Override
  public ListenableFuture<PackingPlans.PackingPlan> getPackingPlan(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.PACKING_PLAN, topologyName,
        PackingPlans.PackingPlan.newBuilder());
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.PHYSICAL_PLAN, topologyName,
        PhysicalPlans.PhysicalPlan.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyManager.TManagerLocation> getTManagerLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.TMANAGER_LOCATION, topologyName,
        TopologyManager.TManagerLocation.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyManager.MetricsCacheLocation> getMetricsCacheLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.METRICSCACHE_LOCATION, topologyName,
        TopologyManager.MetricsCacheLocation.newBuilder());
  }

  @Override
  public ListenableFuture<CheckpointManager.StatefulConsistentCheckpoints> getStatefulCheckpoints(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, StateLocation.STATEFUL_CHECKPOINT, topologyName,
        CheckpointManager.StatefulConsistentCheckpoints.newBuilder());
  }

  @Override
  public ListenableFuture<Boolean> deleteTManagerLocation(String topologyName) {
    return deleteNode(StateLocation.TMANAGER_LOCATION, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deleteMetricsCacheLocation(String topologyName) {
    return deleteNode(StateLocation.METRICSCACHE_LOCATION, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    return deleteNode(StateLocation.SCHEDULER_LOCATION, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    return deleteNode(StateLocation.EXECUTION_STATE, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    return deleteNode(StateLocation.TOPOLOGY, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deletePackingPlan(String topologyName) {
    return deleteNode(StateLocation.PACKING_PLAN, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    return deleteNode(StateLocation.PHYSICAL_PLAN, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deleteStatefulCheckpoints(String topologyName) {
    return deleteNode(StateLocation.STATEFUL_CHECKPOINT, topologyName);
  }

  @Override
  public ListenableFuture<Boolean> deleteLocks(String topologyName) {
    boolean result = true;
    for (LockName lockName : LockName.values()) {
      String path =
          StateLocation.LOCKS.getNodePath(this.rootAddress, topologyName, lockName.getName());
      ListenableFuture<Boolean> thisResult = deleteNode(path, true);
      try {
        if (!thisResult.get()) {
          result = false;
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.log(Level.WARNING, "Error while waiting on result of delete lock at " + thisResult, e);
      }
    }
    final SettableFuture<Boolean> future = SettableFuture.create();
    safeSetFuture(future, result);
    return future;
  }

  private ListenableFuture<Boolean> deleteNode(StateLocation location, String topologyName) {
    return deleteNode(getStatePath(location, topologyName), false);
  }

  private <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                              StateLocation location,
                                                              String topologyName,
                                                              Message.Builder builder) {
    return getNodeData(watcher, getStatePath(location, topologyName), builder);
  }

  /**
   * Returns all information stored in the StateManager. This is a utility method used for debugging
   * while developing. To invoke, run:
   *
   *  bazel run heron/statemgrs/src/java:localfs-statemgr-unshaded -- \
   *    &lt;topology-name&gt; [new_instance_distribution]
   *
   * If a new_instance_distribution is provided, the instance distribution will be updated to
   * trigger a scaling event. For example:
   *
   *  bazel run heron/statemgrs/src/java:localfs-statemgr-unshaded -- \
   *    ExclamationTopology 1:word:3:0:exclaim1:2:0:exclaim1:1:0
   *
   */
  protected void doMain(String[] args, Config config)
      throws ExecutionException, InterruptedException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {

    if (args.length < 1) {
      throw new RuntimeException(String.format(
          "Usage: java %s <topology_name> - view state manager details for a topology",
          this.getClass().getCanonicalName()));
    }

    String topologyName = args[0];
    print("==> State Manager root path: %s",
        config.getStringValue(Key.STATEMGR_ROOT_PATH));

    initialize(config);

    if (isTopologyRunning(topologyName).get()) {
      print("==> Topology %s found", topologyName);
      try {
        print("==> Topology:\n%s", getTopology(null, topologyName).get());
      } catch (ExecutionException e) {
        print("Topology node not found %s", e.getMessage());
      }
      try {
        print("==> ExecutionState:\n%s", getExecutionState(null, topologyName).get());
      } catch (ExecutionException e) {
        print("ExecutionState node not found %s", e.getMessage());
      }
      try {
        print("==> SchedulerLocation:\n%s",
                getSchedulerLocation(null, topologyName).get());
      } catch (ExecutionException e) {
        print("SchedulerLocation node not found %s", e.getMessage());
      }
      try {
        print("==> TManagerLocation:\n%s", getTManagerLocation(null, topologyName).get());
      } catch (ExecutionException e) {
        print("TManagerLocation node not found %s", e.getMessage());
      }
      try {
        print("==> MetricsCacheLocation:\n%s", getMetricsCacheLocation(null, topologyName).get());
      } catch (ExecutionException e) {
        print("MetricsCacheLocation node not found %s", e.getMessage());
      }
      try {
        print("==> PackingPlan:\n%s", getPackingPlan(null, topologyName).get());
      } catch (ExecutionException e) {
        print("PackingPlan node not found %s", e.getMessage());
      }
      try {
        print("==> PhysicalPlan:\n%s", getPhysicalPlan(null, topologyName).get());
      } catch (ExecutionException e) {
        print("PhysicalPlan node not found %s", e.getMessage());
      }
    } else {
      print("==> Topology %s not found under %s",
          topologyName, config.getStringValue(Key.STATEMGR_ROOT_PATH));
    }
  }

  protected void print(String format, Object... values) {
    System.out.println(String.format(format, values));
  }
}
