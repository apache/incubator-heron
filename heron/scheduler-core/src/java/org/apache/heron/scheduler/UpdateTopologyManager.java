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

package org.apache.heron.scheduler;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.PackingPlanProtoDeserializer;
import org.apache.heron.spi.packing.PackingPlanProtoSerializer;
import org.apache.heron.spi.scheduler.IScalable;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.statemgr.Lock;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.NetworkUtils;
import org.apache.heron.spi.utils.TManagerException;
import org.apache.heron.spi.utils.TManagerUtils;

import static org.apache.heron.api.Config.TOPOLOGY_UPDATE_DEACTIVATE_WAIT_SECS;
import static org.apache.heron.api.Config.TOPOLOGY_UPDATE_REACTIVATE_WAIT_SECS;

/**
 * Class that is able to update a topology. This includes changing the parallelism of
 * topology components
 */
public class UpdateTopologyManager implements Closeable {
  private static final Logger LOG = Logger.getLogger(UpdateTopologyManager.class.getName());

  private Config config;
  private Config runtime;
  private Optional<IScalable> scalableScheduler;
  private PackingPlanProtoDeserializer deserializer;
  private ScheduledThreadPoolExecutor reactivateExecutorService;

  public UpdateTopologyManager(Config config, Config runtime,
                               Optional<IScalable> scalableScheduler) {
    this.config = config;
    this.runtime = runtime;
    this.scalableScheduler = scalableScheduler;
    this.deserializer = new PackingPlanProtoDeserializer();
    this.reactivateExecutorService = new ScheduledThreadPoolExecutor(1);
    this.reactivateExecutorService.setMaximumPoolSize(1);
  }

  @Override
  public void close() {
    this.reactivateExecutorService.shutdownNow();
  }

  /**
   * Scales the topology out or in based on the proposedPackingPlan
   *
   * @param existingProtoPackingPlan the current plan. If this isn't what's found in the state
   * manager, the update will fail
   * @param proposedProtoPackingPlan packing plan to change the topology to
   */
  public void updateTopology(final PackingPlans.PackingPlan existingProtoPackingPlan,
                             final PackingPlans.PackingPlan proposedProtoPackingPlan)
      throws ExecutionException, InterruptedException, ConcurrentModificationException {
    String topologyName = Runtime.topologyName(runtime);
    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);
    Lock lock = stateManager.getLock(topologyName, IStateManager.LockName.UPDATE_TOPOLOGY);

    if (lock.tryLock(5, TimeUnit.SECONDS)) {
      try {
        PackingPlans.PackingPlan foundPackingPlan = getPackingPlan(stateManager, topologyName);

        if (!deserializer.fromProto(existingProtoPackingPlan)
            .equals(deserializer.fromProto(foundPackingPlan))) {
          throw new ConcurrentModificationException(String.format(
              "The packing plan in state manager is not the same as the submitted existing "
                  + "packing plan for topology %s. Another actor has changed it and has likely"
                  + "performed an update on it. Failing this request, try again once other "
                  + "update is complete", topologyName));
        }

        updateTopology(existingProtoPackingPlan, proposedProtoPackingPlan, stateManager);
      } finally {
        lock.unlock();
      }
    } else {
      throw new ConcurrentModificationException(String.format(
          "The update lock can not be obtained for topology %s. Another actor is performing an "
              + "update on it. Failing this request, try again once current update is complete",
          topologyName));
    }
  }

  private void updateTopology(final PackingPlans.PackingPlan existingProtoPackingPlan,
                              final PackingPlans.PackingPlan proposedProtoPackingPlan,
                              SchedulerStateManagerAdaptor stateManager)
      throws ExecutionException, InterruptedException {
    String topologyName = Runtime.topologyName(runtime);
    PackingPlan existingPackingPlan = deserializer.fromProto(existingProtoPackingPlan);
    PackingPlan proposedPackingPlan = deserializer.fromProto(proposedProtoPackingPlan);

    Preconditions.checkArgument(proposedPackingPlan.getContainers().size() > 0, String.format(
        "proposed packing plan must have at least 1 container %s", proposedPackingPlan));

    ContainerDelta containerDelta = new ContainerDelta(
        existingPackingPlan.getContainers(), proposedPackingPlan.getContainers());
    int newContainerCount = containerDelta.getContainersToAdd().size();
    int removableContainerCount = containerDelta.getContainersToRemove().size();

    String message = String.format("Topology change requires %s new containers and removing %s "
            + "existing containers, but the scheduler does not support scaling, aborting. "
            + "Existing packing plan: %s, proposed packing plan: %s",
        newContainerCount, removableContainerCount, existingPackingPlan, proposedPackingPlan);
    Preconditions.checkState(newContainerCount + removableContainerCount == 0
        || scalableScheduler.isPresent(), message);

    TopologyAPI.Topology topology = getTopology(stateManager, topologyName);
    boolean initiallyRunning = topology.getState() == TopologyAPI.TopologyState.RUNNING;

    // deactivate and sleep
    if (initiallyRunning) {
      // Update the topology since the state should have changed from RUNNING to PAUSED
      // Will throw exceptions internally if tmanager fails to deactivate
      deactivateTopology(stateManager, topology, proposedPackingPlan);
    }

    Set<PackingPlan.ContainerPlan> updatedContainers =
        new HashSet<>(proposedPackingPlan.getContainers());
    // request new resources if necessary. Once containers are allocated we should make the changes
    // to state manager quickly, otherwise the scheduler might penalize for thrashing on start-up
    if (newContainerCount > 0 && scalableScheduler.isPresent()) {
      Set<PackingPlan.ContainerPlan> containersToAdd = containerDelta.getContainersToAdd();
      Set<PackingPlan.ContainerPlan> containersAdded =
          scalableScheduler.get().addContainers(containersToAdd);
      // Update the PackingPlan with new container-ids
      if (containersAdded != null) {
        if (containersAdded.size() != containersToAdd.size()) {
          throw new RuntimeException("Scheduler failed to add requested containers. Requested "
              + containersToAdd.size() + ", added " + containersAdded.size() + ". "
                  + "The topology can be in a strange stage. "
                  + "Please check carefully or redeploy the topology !!");
        }
        updatedContainers.removeAll(containersToAdd);
        updatedContainers.addAll(containersAdded);
      }
    }

    PackingPlan updatedPackingPlan =
        new PackingPlan(proposedPackingPlan.getId(), updatedContainers);
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    PackingPlans.PackingPlan updatedProtoPackingPlan = serializer.toProto(updatedPackingPlan);
    LOG.fine("The updated Packing Plan: " + updatedProtoPackingPlan);

    // update packing plan to trigger the scaling event
    logInfo("Update new PackingPlan: %s",
        stateManager.updatePackingPlan(updatedProtoPackingPlan, topologyName));

    // reactivate topology
    if (initiallyRunning) {
      // wait before reactivating to give the tmanager a chance to receive the packing update and
      // delete the packing plan. Instead we could message tmanager to invalidate the physical plan
      // and/or possibly even update the packing plan directly
      SysUtils.sleep(Duration.ofSeconds(10));
      // Will throw exceptions internally if tmanager fails to deactivate
      reactivateTopology(stateManager, topology, removableContainerCount);
    }

    if (removableContainerCount > 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().removeContainers(containerDelta.getContainersToRemove());
    }
  }

  @VisibleForTesting
  void deactivateTopology(SchedulerStateManagerAdaptor stateManager,
                          final TopologyAPI.Topology topology,
                          PackingPlan proposedPackingPlan)
      throws InterruptedException, TManagerException {

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    long deactivateSleepSeconds = TopologyUtils.getConfigWithDefault(
        topologyConfig, TOPOLOGY_UPDATE_DEACTIVATE_WAIT_SECS, 0L);

    logInfo("Deactivating topology %s before handling update request", topology.getName());
    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
    TManagerUtils.transitionTopologyState(
            topology.getName(), TManagerUtils.TManagerCommand.DEACTIVATE, stateManager,
            TopologyAPI.TopologyState.RUNNING, TopologyAPI.TopologyState.PAUSED, tunnelConfig);
    if (deactivateSleepSeconds > 0) {
      logInfo("Deactivated topology %s. Sleeping for %d seconds before handling update request",
          topology.getName(), deactivateSleepSeconds);
      Thread.sleep(deactivateSleepSeconds * 1000);
    } else {
      logInfo("Deactivated topology %s.", topology.getName());
    }
  }

  @VisibleForTesting
  void reactivateTopology(SchedulerStateManagerAdaptor stateManager,
                          TopologyAPI.Topology topology,
                          int removableContainerCount)
      throws ExecutionException, InterruptedException {

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    long waitSeconds = TopologyUtils.getConfigWithDefault(
        topologyConfig, TOPOLOGY_UPDATE_REACTIVATE_WAIT_SECS, 10 * 60L);
    long delaySeconds = 10;

    logInfo("Waiting for physical plan to be set before re-activating topology %s. "
            + "Will wait up to %s seconds for packing plan to be reset",
        topology.getName(), waitSeconds);
    Enabler enabler = new Enabler(stateManager, topology, waitSeconds, removableContainerCount);
    Future<?> future = this.reactivateExecutorService
        .scheduleWithFixedDelay(enabler, 0, delaySeconds, TimeUnit.SECONDS);
    enabler.setFutureRunnable(future);

    try {
      future.get(waitSeconds, TimeUnit.SECONDS);
    } catch (CancellationException e) {
      LOG.fine("Task to re-enable was cancelled.");
    } catch (TimeoutException e) {
      throw new ExecutionException("Timeout waiting for topology to be enabled.", e);
    }
  }

  private final class Enabler implements Runnable {
    private SchedulerStateManagerAdaptor stateManager;
    private String topologyName;
    private int removableContainerCount;
    private long timeoutTime;
    private Future<?> futureRunnable;
    private volatile boolean cancelled = false;

    private Enabler(SchedulerStateManagerAdaptor stateManager,
                    TopologyAPI.Topology topology,
                    long timeoutSeconds,
                    int removableContainerCount) {
      this.stateManager = stateManager;
      this.removableContainerCount = removableContainerCount;
      this.topologyName = topology.getName();
      this.timeoutTime = System.currentTimeMillis() + timeoutSeconds * 1000;
    }

    private synchronized void setFutureRunnable(Future<?> futureRunnable) {
      this.futureRunnable = futureRunnable;
      if (this.cancelled) {
        cancel();
      }
    }

    private void cancel() {
      this.cancelled = true;
      if (this.futureRunnable != null && !this.futureRunnable.isCancelled()) {
        logInfo("Cancelling Topology reactivation task for topology %s", topologyName);
        this.futureRunnable.cancel(true);
      }
    }

    @Override
    public synchronized void run() {
      PhysicalPlans.PhysicalPlan physicalPlan = stateManager.getPhysicalPlan(topologyName);

      if (physicalPlan != null) {
        logInfo("Received physical plan for topology %s. "
            + "Reactivating topology after scaling event", topologyName);
        NetworkUtils.TunnelConfig tunnelConfig =
            NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
        try {
          TManagerUtils.transitionTopologyState(
              topologyName, TManagerUtils.TManagerCommand.ACTIVATE, stateManager,
              TopologyAPI.TopologyState.PAUSED, TopologyAPI.TopologyState.RUNNING, tunnelConfig);
        } catch (TManagerException e) {
          if (removableContainerCount < 1) {
            throw new TopologyRuntimeManagementException(String.format(
                "Topology reactivation failed for topology %s after topology update",
                topologyName), e);
          } else {
            throw new TopologyRuntimeManagementException(String.format(
                "Topology reactivation failed for topology %s after topology "
                  + "update but before releasing %d no longer used containers",
                topologyName, removableContainerCount), e);
          }
        } finally {
          cancel();
        }
      }

      if (System.currentTimeMillis() > this.timeoutTime) {
        LOG.warning(String.format("New physical plan not received within configured timeout for "
            + "topology %s. Not reactivating", topologyName));
        cancel();
      } else {
        logInfo("Couldn't fetch physical plan for topology %s. This is probably because stream "
            + "managers are still registering with TManager. Will sleep and try again",
            topologyName);
      }
    }
  }

  @VisibleForTesting
  PackingPlans.PackingPlan getPackingPlan(SchedulerStateManagerAdaptor stateManager,
                                          String topologyName) {
    return stateManager.getPackingPlan(topologyName);
  }

  /**
   * Returns the topology. It's key that we get the topology from the physical plan to reflect any
   * state changes since launch. The stateManager.getTopology(name) method returns the topology from
   * the time of submission. See additional commentary in topology.proto and physical_plan.proto.
   */
  @VisibleForTesting
  TopologyAPI.Topology getTopology(SchedulerStateManagerAdaptor stateManager, String topologyName) {
    return stateManager.getPhysicalPlan(topologyName).getTopology();
  }

  /**
   * Given both a current and proposed set of containers, determines the set of containers to be
   * added and those to be removed. Whether to add or remove a container is determined by the id of
   * the container. Proposed containers with an id not in the existing set are to be added, while
   * current container ids not in the proposed set are to be removed.
   *
   * It is important to note that the container comparison is done by id only, and does not include
   * the InstancePlans in the container, which for a given container might change in the proposed
   * plan. Changing the size of a container is not supported and will be ignored.
   */
  @VisibleForTesting
  static class ContainerDelta {
    private final Set<PackingPlan.ContainerPlan> containersToAdd;
    private final Set<PackingPlan.ContainerPlan> containersToRemove;

    @VisibleForTesting
    ContainerDelta(Set<PackingPlan.ContainerPlan> currentContainers,
                   Set<PackingPlan.ContainerPlan> proposedContainers) {

      Set<Integer> currentContainerIds = toIdSet(currentContainers);
      Set<Integer> proposedContainerIds = toIdSet(proposedContainers);

      Set<PackingPlan.ContainerPlan> toAdd = new HashSet<>();
      for (PackingPlan.ContainerPlan proposedContainerPlan : proposedContainers) {
        if (!currentContainerIds.contains(proposedContainerPlan.getId())) {
          toAdd.add(proposedContainerPlan);
        }
      }
      this.containersToAdd = Collections.unmodifiableSet(toAdd);

      Set<PackingPlan.ContainerPlan> toRemove = new HashSet<>();
      for (PackingPlan.ContainerPlan currentContainerPlan : currentContainers) {
        if (!proposedContainerIds.contains(currentContainerPlan.getId())) {
          toRemove.add(currentContainerPlan);
        }
      }
      this.containersToRemove = Collections.unmodifiableSet(toRemove);
    }

    @VisibleForTesting
    Set<PackingPlan.ContainerPlan> getContainersToRemove() {
      return containersToRemove;
    }

    @VisibleForTesting
    Set<PackingPlan.ContainerPlan> getContainersToAdd() {
      return containersToAdd;
    }
  }

  private static Set<Integer> toIdSet(Set<PackingPlan.ContainerPlan> containers) {
    Set<Integer> currentContainerMap = new HashSet<>();
    for (PackingPlan.ContainerPlan container : containers) {
      currentContainerMap.add(container.getId());
    }
    return currentContainerMap;
  }

  private static void logInfo(String format, Object... values) {
    LOG.info(String.format(format, values));
  }
  private static void logFine(String format, Object... values) {
    LOG.fine(String.format(format, values));
  }
}
