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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.protobuf.Descriptors;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.Config;
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

  public UpdateTopologyManager(Config runtime, Optional<ScalableScheduler> scalableScheduler) {
    this.runtime = runtime;
    this.scalableScheduler = scalableScheduler;
  }

  /**
   * Scales the topology out or in based on the proposedPackingPlan
   * @param existingPackingPlan the current plan. If this isn't what's found in the state manager,
   * the update will fail
   * @param proposedPackingPlan packing plan to change the topology to
   */
  public void updateTopology(PackingPlans.PackingPlan existingPackingPlan,
                             PackingPlans.PackingPlan proposedPackingPlan)
      throws ExecutionException, InterruptedException {
    String topologyName = Runtime.topologyName(runtime);

    Map<String, Integer> proposedChanges = parallelismChanges(
        existingPackingPlan.getInstanceDistribution(),
        proposedPackingPlan.getInstanceDistribution());

    int existingContainerCount = parseInstanceDistributionContainers(
        existingPackingPlan.getInstanceDistribution()).length;
    int proposedContainerCount = parseInstanceDistributionContainers(
        proposedPackingPlan.getInstanceDistribution()).length;
    Integer containerDelta = proposedContainerCount - existingContainerCount;

    assertTrue(proposedContainerCount > 0,
        "proposed instance distribution must have at least 1 container %s",
        proposedPackingPlan.getInstanceDistribution());

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

    // request new aurora resources if necessary. Once containers are allocated we must make the
    // changes to state manager quickly, otherwise aurora might penalize for thrashing on start-up
    if (containerDelta > 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().addContainers(containerDelta);
    }

    // assert found is same as existing.
    PackingPlans.PackingPlan foundPackingPlan = stateManager.getPackingPlan(topologyName);
    assertTrue(foundPackingPlan.equals(existingPackingPlan),
        "Existing packing plan received does not equal the packing plan found in the state "
        + "manager. Not updating updatedTopology. Received: %s, Found: %s",
        existingPackingPlan, foundPackingPlan);

    // update parallelism in updatedTopology since TMaster checks that
    // Sum(parallelism) == Sum(instances)
    print("==> Deleted existing Topology: %s", stateManager.deleteTopology(topologyName));
    print("==> Set new Topology: %s", stateManager.setTopology(updatedTopology, topologyName));

    // update packing plan to trigger the scaling event
    print("==> Deleted existing packing plan: %s", stateManager.deletePackingPlan(topologyName));
    print("==> Set new PackingPlan: %s",
        stateManager.setPackingPlan(proposedPackingPlan, topologyName));

    // delete the physical plan so TMaster doesn't try to re-establish it on start-up.
    print("==> Deleted Physical Plan: %s", stateManager.deletePhysicalPlan(topologyName));

    if (containerDelta < 0 && scalableScheduler.isPresent()) {
      scalableScheduler.get().removeContainers(existingContainerCount, -containerDelta);
    }
  }

  // Given the existing and proposed instance distribution, verify the proposal and return only
  // the changes being requested.
  private Map<String, Integer> parallelismChanges(String existingInstanceDistribution,
                                                  String proposedInstanceDistribution) {
    Map<String, Integer> existingParallelism =
        parseInstanceDistribution(existingInstanceDistribution);
    Map<String, Integer> proposedParallelism =
        parseInstanceDistribution(proposedInstanceDistribution);
    Map<String, Integer> parallelismChanges = new HashMap<>();

    for (String componentName : proposedParallelism.keySet()) {
      Integer newParallelism = proposedParallelism.get(componentName);
      assertTrue(existingParallelism.containsKey(componentName),
          "key %s in proposed instance distribution %s not found in "
              + "current instance distribution %s",
          componentName, proposedInstanceDistribution, existingInstanceDistribution);
      assertTrue(newParallelism > 0,
          "Non-positive parallelism (%s) for component %s found in instance distribution %s",
          newParallelism, componentName, proposedInstanceDistribution);

      if (!newParallelism.equals(existingParallelism.get(componentName))) {
        parallelismChanges.put(componentName, newParallelism);
      }
    }
    return parallelismChanges;
  }

  // given a string like "1:word:3:0:exclaim1:2:0:exclaim1:1:0,2:exclaim1:4:0" returns a map of
  // { word -> 1, explain1 -> 3 }
  private Map<String, Integer> parseInstanceDistribution(String instanceDistribution) {
    Map<String, Integer> componentParallelism = new HashMap<>();
    String[] containers = parseInstanceDistributionContainers(instanceDistribution);
    for (String container : containers) {
      String[] tokens = container.split(":");
      assertTrue(tokens.length > 3 && (tokens.length - 1) % 3 == 0,
          "Invalid instance distribution format. Expected componentId "
              + "followed by instance triples: %s", instanceDistribution);
      Set<String> idsFound = new HashSet<>();
      for (int i = 1; i < tokens.length; i += 3) {
        String instanceName = tokens[i];
        String instanceId = tokens[i + 1];
        assertTrue(!idsFound.contains(instanceId),
            "Duplicate instanceId (%s) found in instance distribution %s for instance %s %s",
            instanceId, instanceDistribution, instanceName, i);
        idsFound.add(instanceId);
        Integer occurrences = componentParallelism.getOrDefault(instanceName, 0);
        componentParallelism.put(instanceName, occurrences + 1);
      }
    }
    return componentParallelism;
  }

  private static String[] parseInstanceDistributionContainers(String instanceDistribution) {
    return instanceDistribution.split(",");
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
