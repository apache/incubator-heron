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
package com.twitter.heron.scheduler;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.protobuf.Descriptors;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TMasterUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

import static com.twitter.heron.api.Config.TOPOLOGY_UPDATE_DEACTIVATE_WAIT_SECS;
import static com.twitter.heron.api.Config.TOPOLOGY_UPDATE_REACTIVATE_WAIT_SECS;

/**
 * Class that is able to update a topology. This includes changing the parallelism of
 * topology components
 */
public class UpdateTopologyManager implements Closeable {
  private static final Logger LOG = Logger.getLogger(UpdateTopologyManager.class.getName());

  private Config runtime;
  private Optional<IScalable> scalableScheduler;
  private PackingPlanProtoDeserializer deserializer;
  private ScheduledThreadPoolExecutor reactivateExecutorService;

  public UpdateTopologyManager(Config runtime, Optional<IScalable> scalableScheduler) {
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
   * @param existingProtoPackingPlan the current plan. If this isn't what's found in the state manager,
   * the update will fail
   * @param proposedProtoPackingPlan packing plan to change the topology to
   */
  public void updateTopology(final PackingPlans.PackingPlan existingProtoPackingPlan,
                             final PackingPlans.PackingPlan proposedProtoPackingPlan)
      throws ExecutionException, InterruptedException {
    String topologyName = Runtime.topologyName(runtime);
    PackingPlan existingPackingPlan = deserializer.fromProto(existingProtoPackingPlan);
    PackingPlan proposedPackingPlan = deserializer.fromProto(proposedProtoPackingPlan);

    assertTrue(proposedPackingPlan.getContainers().size() > 0,
        "proposed packing plan must have at least 1 container %s", proposedPackingPlan);

    ContainerDelta containerDelta = new ContainerDelta(
        existingPackingPlan.getContainers(), proposedPackingPlan.getContainers());
    int newContainerCount = containerDelta.getContainersToAdd().size();
    int removableContainerCount = containerDelta.getContainersToRemove().size();

    String message = String.format("Topology change requires %s new containers and removing %s "
            + "existing containers, but the scheduler does not support scaling, aborting. "
            + "Existing packing plan: %s, proposed packing plan: %s",
        newContainerCount, removableContainerCount, existingPackingPlan, proposedPackingPlan);
    assertTrue(newContainerCount + removableContainerCount == 0 || scalableScheduler.isPresent(),
        message);

    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    // fetch the topology, which will need to be updated
    TopologyAPI.Topology updatedTopology =
        getUpdatedTopology(topologyName, proposedPackingPlan, stateManager);

    // deactivate and sleep
    deactivateTopology(stateManager, updatedTopology);

    // request new resources if necessary. Once containers are allocated we should make the changes
    // to state manager quickly, otherwise the scheduler might penalize for thrashing on start-up
    if (newContainerCount > 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().addContainers(containerDelta.getContainersToAdd());
    }

    // update parallelism in updatedTopology since TMaster checks that
    // Sum(parallelism) == Sum(instances)
    logFine("Deleted existing Topology: %s", stateManager.deleteTopology(topologyName));
    logFine("Set new Topology: %s", stateManager.setTopology(updatedTopology, topologyName));

    // update packing plan to trigger the scaling event
    logFine("Deleted existing packing plan: %s", stateManager.deletePackingPlan(topologyName));
    logFine("Set new PackingPlan: %s",
        stateManager.setPackingPlan(proposedProtoPackingPlan, topologyName));

    // delete the physical plan so TMaster doesn't try to re-establish it on start-up.
    logFine("Deleted Physical Plan: %s", stateManager.deletePhysicalPlan(topologyName));

    // reactivate topology
    reactivateTopology(stateManager, updatedTopology, removableContainerCount);

    if (removableContainerCount > 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().removeContainers(containerDelta.getContainersToRemove());
    }
  }

  @VisibleForTesting
  void deactivateTopology(SchedulerStateManagerAdaptor stateManager,
                          final TopologyAPI.Topology topology)
      throws InterruptedException {

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    long deactivateSleepSeconds = TopologyUtils.getConfigWithDefault(
        topologyConfig, TOPOLOGY_UPDATE_DEACTIVATE_WAIT_SECS, 0L);

    logInfo("Deactivating topology %s before handling update request", topology.getName());
    assertTrue(TMasterUtils.transitionTopologyState(
            topology.getName(), TMasterUtils.TMasterCommand.DEACTIVATE, stateManager,
            TopologyAPI.TopologyState.RUNNING, TopologyAPI.TopologyState.PAUSED),
        "Failed to deactivate topology %s. Aborting update request", topology.getName());

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
                          int removableContainerCount) {

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    long waitSeconds = TopologyUtils.getConfigWithDefault(
        topologyConfig, TOPOLOGY_UPDATE_REACTIVATE_WAIT_SECS, 10 * 60L);
    long delaySeconds = 10;

    logInfo("Waiting for packing plan to be set before re-activating topology %s. "
            + "Will wait up to %s seconds for packing plan to be reset",
        topology.getName(), waitSeconds);
    Enabler enabler = new Enabler(stateManager, topology, waitSeconds, removableContainerCount);
    Future<?> future = this.reactivateExecutorService
        .scheduleWithFixedDelay(enabler, 0, delaySeconds, TimeUnit.SECONDS);
    enabler.setFutureRunnable(future);
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
        logInfo("Received packing plan for topology %s. "
            + "Reactivating topology after scaling event", topologyName);
        boolean reactivated = TMasterUtils.transitionTopologyState(
            topologyName, TMasterUtils.TMasterCommand.ACTIVATE, stateManager,
            TopologyAPI.TopologyState.PAUSED, TopologyAPI.TopologyState.RUNNING);

        cancel();

        if (removableContainerCount < 1) {
          assertTrue(reactivated,
              "Topology reactivation failed for topology %s after topology update", topologyName);
        } else {
          assertTrue(reactivated, "Topology reactivation failed for topology %s after topology "
                  + "update but before releasing %d no longer used containers",
              topologyName, removableContainerCount);
        }
        return;
      } else {
        logInfo("Received null packing plan for topology %s.", topologyName);
      }

      if (System.currentTimeMillis() > this.timeoutTime) {
        LOG.warning(String.format("New packing plan not received within configured timeout for "
            + "topology %s. Not reactivating", topologyName));
        cancel();
      }
    }
  }

  @VisibleForTesting
  TopologyAPI.Topology getUpdatedTopology(String topologyName,
                                          PackingPlan proposedPackingPlan,
                                          SchedulerStateManagerAdaptor stateManager) {
    TopologyAPI.Topology updatedTopology = stateManager.getTopology(topologyName);
    Map<String, Integer> proposedComponentCounts = proposedPackingPlan.getComponentCounts();
    return mergeTopology(updatedTopology, proposedComponentCounts);
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

  /**
   * For each of the components with changed parallelism we need to update the Topology configs to
   * represent the change.
   */
  @VisibleForTesting
  static TopologyAPI.Topology mergeTopology(TopologyAPI.Topology topology,
                                            Map<String, Integer> proposedComponentCounts) {
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
    for (String componentName : proposedComponentCounts.keySet()) {
      Integer parallelism = proposedComponentCounts.get(componentName);

      boolean updated = false;
      for (TopologyAPI.Bolt.Builder boltBuilder : builder.getBoltsBuilderList()) {
        if (updateComponent(boltBuilder.getCompBuilder(), componentName, parallelism)) {
          updated = true;
          break;
        }
      }

      if (!updated) {
        for (TopologyAPI.Spout.Builder spoutBuilder : builder.getSpoutsBuilderList()) {
          if (updateComponent(spoutBuilder.getCompBuilder(), componentName, parallelism)) {
            break;
          }
        }
      }
    }

    return builder.build();
  }

  /**
   * Go through all fields in the component builder until one is found where "name=componentName".
   * When found, go through the confs in the conf builder to find the parallelism field and update
   * that.
   *
   * @return true if the component was found and updated, which might not always happen. For example
   * if the component is a spout, it won't be found if a bolt builder is passed.
   */
  private static boolean updateComponent(TopologyAPI.Component.Builder compBuilder,
                                         String componentName,
                                         int parallelism) {
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry
        : compBuilder.getAllFields().entrySet()) {
      if (entry.getKey().getName().equals("name") && componentName.equals(entry.getValue())) {
        TopologyAPI.Config.Builder confBuilder = compBuilder.getConfigBuilder();
        boolean keyFound = false;
        // no way to get a KeyValue builder with a get(key) so we have to iterate until found.
        for (TopologyAPI.Config.KeyValue.Builder kvBuilder : confBuilder.getKvsBuilderList()) {
          if (kvBuilder.getKey().equals(
              com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM)) {
            kvBuilder.setValue(Integer.toString(parallelism));
            keyFound = true;
            break;
          }
        }
        if (!keyFound) {
          TopologyAPI.Config.KeyValue.Builder kvBuilder =
              TopologyAPI.Config.KeyValue.newBuilder();
          kvBuilder.setKey(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM);
          kvBuilder.setValue(Integer.toString(parallelism));
          confBuilder.addKvs(kvBuilder);
        }
        return true;
      }
    }
    return false;
  }

  private static Set<Integer> toIdSet(Set<PackingPlan.ContainerPlan> containers) {
    Set<Integer> currentContainerMap = new HashSet<>();
    for (PackingPlan.ContainerPlan container : containers) {
      currentContainerMap.add(container.getId());
    }
    return currentContainerMap;
  }

  private static void assertTrue(boolean condition, String message, Object... values) {
    if (!condition) {
      throw new RuntimeException("ERROR: " + String.format(message, values));
    }
  }

  private static void logInfo(String format, Object... values) {
    LOG.info(String.format(format, values));
  }
  private static void logFine(String format, Object... values) {
    LOG.fine(String.format(format, values));
  }
}
