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

    Integer proposedContainerCount = proposedPackingPlan.getContainers().size();
    Integer existingContainerCount = existingPackingPlan.getContainers().size();
    Integer containerDelta = proposedContainerCount - existingContainerCount;

    assertTrue(proposedContainerCount > 0,
        "proposed packing plan must have at least 1 container %s", proposedPackingPlan);
    assertTrue(containerDelta == 0 || scalableScheduler.isPresent(),
        "Topology change requires scaling containers by %s but scheduler does not support "
        + "scaling, aborting. Existing packing plan: %s, proposed packing plan: %s",
        containerDelta, existingPackingPlan, proposedPackingPlan);

    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    // assert found PackingPlan is same as existing PackingPlan.
    PackingPlans.PackingPlan foundPackingPlan = stateManager.getPackingPlan(topologyName);
    assertTrue(foundPackingPlan.equals(existingProtoPackingPlan),
        "Existing packing plan received does not equal the packing plan found in the state "
        + "manager. Not updating updatedTopology. Received: %s, Found: %s",
        existingProtoPackingPlan, foundPackingPlan);

    // fetch the topology, which will need to be updated
    TopologyAPI.Topology updatedTopology = stateManager.getTopology(topologyName);
    Map<String, Integer> proposedComponentCounts = proposedPackingPlan.getComponentCounts();
    for (String componentName : proposedComponentCounts.keySet()) {
      Integer parallelism = proposedComponentCounts.get(componentName);
      updatedTopology = mergeTopology(updatedTopology, componentName, parallelism);
    }

    // request new resources if necessary. Once containers are allocated we should make the changes
    // to state manager quickly, otherwise the scheduler might penalize for thrashing on start-up
    if (containerDelta > 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().addContainers(containerDelta);
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

    if (containerDelta < 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().removeContainers(existingContainerCount, -containerDelta);
    }
  }

  private static TopologyAPI.Topology mergeTopology(TopologyAPI.Topology topology,
                                                    String componentName,
                                                    int parallelism) {
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
    for (TopologyAPI.Bolt.Builder boltBuilder : builder.getBoltsBuilderList()) {
      updateComponent(boltBuilder.getCompBuilder(), componentName, parallelism);
    }
    for (TopologyAPI.Spout.Builder spoutBuilder : builder.getSpoutsBuilderList()) {
      updateComponent(spoutBuilder.getCompBuilder(), componentName, parallelism);
    }
    return builder.build();
  }

  private static void updateComponent(TopologyAPI.Component.Builder compBuilder,
                                      String componentName,
                                      int parallelism) {
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

  private void assertTrue(boolean condition, String message, Object... values) {
    if (!condition) {
      throw new RuntimeException("ERROR: " + String.format(message, values));
    }
  }

  private void logFine(String format, Object... values) {
    LOG.fine(String.format(format, values));
  }
}
