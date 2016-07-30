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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.protobuf.Descriptors;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;

/**
 * Class that is able to update a topology. This includes changing the parallelism of
 * topology components
 */
public class UpdateTopologyManager {
  private static final Logger LOG = Logger.getLogger(UpdateTopologyManager.class.getName());

  private Config runtime;
  private Optional<ScalableScheduler> scalableScheduler;
  private PackingPlanProtoDeserializer deserializer;

  public UpdateTopologyManager(Config runtime, Optional<ScalableScheduler> scalableScheduler) {
    this.runtime = runtime;
    this.scalableScheduler = scalableScheduler;
    this.deserializer = new PackingPlanProtoDeserializer();
  }

  /**
   * Scales the topology out or in based on the proposedPackingPlan
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

    Map<String, Integer> proposedChanges = parallelismDeltas(
        existingPackingPlan, proposedPackingPlan);

    int existingContainerCount = existingPackingPlan.getContainers().size();
    int proposedContainerCount = proposedPackingPlan.getContainers().size();
    Integer containerDelta = proposedContainerCount - existingContainerCount;

    assertTrue(proposedContainerCount > 0,
        "proposed packing plan must have at least 1 container %s", proposedPackingPlan);

    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    assertTrue(containerDelta == 0 || scalableScheduler.isPresent(),
        "Topology change requires scaling containers by %s but scheduler does not support "
        + "scaling, aborting. Existing packing plan: %s, proposed packing plan: %s",
        containerDelta, existingPackingPlan, proposedPackingPlan);

    // fetch the topology, which will need to be updated
    TopologyAPI.Topology updatedTopology = stateManager.getTopology(topologyName);
    for (String componentName : proposedChanges.keySet()) {
      Integer parallelism = proposedChanges.get(componentName);
      updatedTopology = mergeTopology(updatedTopology, componentName, parallelism);
    }

    // assert found is same as existing.
    PackingPlans.PackingPlan foundPackingPlan = stateManager.getPackingPlan(topologyName);
    assertTrue(foundPackingPlan.equals(existingProtoPackingPlan),
        "Existing packing plan received does not equal the packing plan found in the state "
        + "manager. Not updating updatedTopology. Received: %s, Found: %s",
        existingProtoPackingPlan, foundPackingPlan);

    // request new resources if necessary. Once containers are allocated we should make the changes
    // to state manager quickly, otherwise the scheduler might penalize for thrashing on start-up
    if (containerDelta > 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().addContainers(containerDelta);
    }

    // update parallelism in updatedTopology since TMaster checks that
    // Sum(parallelism) == Sum(instances)
    print("==> Deleted existing Topology: %s", stateManager.deleteTopology(topologyName));
    print("==> Set new Topology: %s", stateManager.setTopology(updatedTopology, topologyName));

    // update packing plan to trigger the scaling event
    print("==> Deleted existing packing plan: %s", stateManager.deletePackingPlan(topologyName));
    print("==> Set new PackingPlan: %s",
        stateManager.setPackingPlan(proposedProtoPackingPlan, topologyName));

    // delete the physical plan so TMaster doesn't try to re-establish it on start-up.
    print("==> Deleted Physical Plan: %s", stateManager.deletePhysicalPlan(topologyName));

    if (containerDelta < 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().removeContainers(existingContainerCount, -containerDelta);
    }
  }

  // Given the existing and proposed instance distribution, verify the proposal and return only
  // the changes being requested.
  private Map<String, Integer> parallelismDeltas(PackingPlan existingPackingPlan,
                                                 PackingPlan proposedPackingPlan) {
    Map<String, Integer> existingComponentCounts = existingPackingPlan.getComponentCounts();
    Map<String, Integer> proposedComponentCounts = proposedPackingPlan.getComponentCounts();

    Map<String, Integer> parallelismChanges = new HashMap<>();

    for (String componentName : proposedComponentCounts.keySet()) {
      Integer newParallelism = proposedComponentCounts.get(componentName);
      assertTrue(existingComponentCounts.containsKey(componentName),
          "component %s in proposed instance distribution not found in "
              + "current instance distribution", componentName);
      assertTrue(newParallelism > 0,
          "Non-positive parallelism (%s) for component %s found in proposed instance distribution",
          newParallelism, componentName);

      if (!newParallelism.equals(existingComponentCounts.get(componentName))) {
        parallelismChanges.put(componentName, newParallelism);
      }
    }
    return parallelismChanges;
  }

  private static TopologyAPI.Topology mergeTopology(TopologyAPI.Topology topology,
                                                    String componentName,
                                                    int parallelism) {
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
    for (int i = 0; i < builder.getBoltsCount(); i++) {
      TopologyAPI.Bolt.Builder boltBuilder = builder.getBoltsBuilder(i);
      TopologyAPI.Component.Builder compBuilder = boltBuilder.getCompBuilder();
      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry
          : compBuilder.getAllFields().entrySet()) {
        if (entry.getKey().getName().equals("name") && componentName.equals(entry.getValue())) {
          TopologyAPI.Config.Builder confBuilder = compBuilder.getConfigBuilder();
          boolean keyFound = false;
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
        }
      }
    }
    return builder.build();
  }

  protected void assertTrue(boolean condition, String message, Object... values) {
    if (!condition) {
      throw new RuntimeException("ERROR: " + String.format(message, values));
    }
  }

  protected void print(String format, Object... values) {
    LOG.fine(String.format(format, values));
  }
}
