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
package com.twitter.heron.healthmgr.resolvers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.resolver.Action;

import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.utils.ReflectionUtils;

public class ScaleUpResolver implements IResolver {
  private static final String BACK_PRESSURE = HealthMgrConstants.METRIC_INSTANCE_BACK_PRESSURE;
  private static final Logger LOG = Logger.getLogger(ScaleUpResolver.class.getName());

  private TopologyProvider topologyProvider;
  private PackingPlanProvider packingPlanProvider;
  private ISchedulerClient schedulerClient;
  private EventManager eventManager;
  private Config config;

  @Inject
  public ScaleUpResolver(TopologyProvider topologyProvider,
                         PackingPlanProvider packingPlanProvider,
                         ISchedulerClient schedulerClient,
                         EventManager eventManager,
                         Config config) {
    this.topologyProvider = topologyProvider;
    this.packingPlanProvider = packingPlanProvider;
    this.schedulerClient = schedulerClient;
    this.eventManager = eventManager;
    this.config = config;
  }

  @Override
  public List<Action> resolve(List<Diagnosis> diagnosis) {
    for (Diagnosis diagnoses : diagnosis) {
      Symptom bpSymptom = diagnoses.getSymptoms().get(BACK_PRESSURE);
      if (bpSymptom == null || bpSymptom.getComponents().isEmpty()) {
        // nothing to fix as there is no back pressure
        continue;
      }

      if (bpSymptom.getComponents().size() > 1) {
        throw new UnsupportedOperationException("Multiple components with back pressure symptom");
      }

      ComponentMetrics bpComponent = bpSymptom.getComponent();
      int newParallelism = computeScaleUpFactor(bpComponent);
      Map<String, Integer> changeRequest = new HashMap<>();
      changeRequest.put(bpComponent.getName(), newParallelism);

      PackingPlan currentPackingPlan = packingPlanProvider.get();
      PackingPlan newPlan = buildNewPackingPlan(changeRequest, currentPackingPlan);
      if (newPlan == null) {
        return null;
      }

      Scheduler.UpdateTopologyRequest updateTopologyRequest =
          Scheduler.UpdateTopologyRequest.newBuilder()
              .setCurrentPackingPlan(getSerializedPlan(currentPackingPlan))
              .setProposedPackingPlan(getSerializedPlan(newPlan))
              .build();

      LOG.info("Sending Updating topology request: " + updateTopologyRequest);
      if (!schedulerClient.updateTopology(updateTopologyRequest)) {
        throw new RuntimeException(String.format("Failed to update topology with Scheduler, " +
            "updateTopologyRequest=%s", updateTopologyRequest));
      }

      TopologyUpdate action = new TopologyUpdate();
      LOG.info("Broadcasting topology update event");
      eventManager.onEvent(action);

      LOG.info("Scheduler updated topology successfully.");

      List<Action> actions = new ArrayList<>();
      actions.add(action);
      return actions;
    }

    return null;
  }

  @VisibleForTesting
  int computeScaleUpFactor(ComponentMetrics componentMetrics) {
    double totalCompBpTime = 0;
    String compName = componentMetrics.getName();
    for (InstanceMetrics instanceMetrics : componentMetrics.getMetrics().values()) {
      double instanceBp = instanceMetrics.getMetricValue(BACK_PRESSURE);
      LOG.info(String.format("Instance:%s, bpTime:%.0f", instanceMetrics.getName(), instanceBp));
      totalCompBpTime += instanceBp;
    }

    LOG.info(String.format("Component: %s, bpTime: %.0f", compName, totalCompBpTime));
    if (totalCompBpTime >= 1000) {
      totalCompBpTime = 999;
    }
    LOG.warning(String.format("Comp:%s, bpTime after filter: %.0f", compName, totalCompBpTime));

    double unusedCapacity = (1.0 * totalCompBpTime) / (1000 - totalCompBpTime);
    // scale up fencing: do not scale more than 4 times the current size
    unusedCapacity = unusedCapacity > 4.0 ? 4.0 : unusedCapacity;
    int parallelism = (int) Math.ceil(componentMetrics.getMetrics().size() * (1 + unusedCapacity));
    LOG.info(String.format("Component's, %s, unused capacity is: %.3f. New parallelism: %d",
        compName, unusedCapacity, parallelism));
    return parallelism;
  }


  @VisibleForTesting
  PackingPlan buildNewPackingPlan(Map<String, Integer> changeRequests,
                                  PackingPlan currentPackingPlan) {
    Map<String, Integer> componentDeltas = new HashMap<>();
    Map<String, Integer> componentCounts = currentPackingPlan.getComponentCounts();
    for (String compName : changeRequests.keySet()) {
      if (!componentCounts.containsKey(compName)) {
        throw new IllegalArgumentException(String.format(
            "Invalid component name in scale up diagnosis: %s. Valid components include: %s",
            compName, Arrays.toString(
                componentCounts.keySet().toArray(new String[componentCounts.keySet().size()]))));
      }

      Integer newValue = changeRequests.get(compName);
      int delta = newValue - componentCounts.get(compName);
      if (delta == 0) {
        LOG.info(String.format("New parallelism for %s is unchanged: %d", compName, newValue));
        continue;
      }

      componentDeltas.put(compName, delta);
    }

    // Create an instance of the packing class
    IRepacking packing = getRepackingClass(Context.repackingClass(config));

    Topology topology = topologyProvider.get();
    try {
      packing.initialize(config, topology);
      PackingPlan packedPlan = packing.repack(currentPackingPlan, componentDeltas);
      return packedPlan;
    } finally {
      SysUtils.closeIgnoringExceptions(packing);
    }
  }

  @VisibleForTesting
  IRepacking getRepackingClass(String repackingClass) {
    IRepacking packing;
    try {
      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(repackingClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate packing instance: " + repackingClass, e);
    }
    return packing;
  }

  @VisibleForTesting
  PackingPlans.PackingPlan getSerializedPlan(PackingPlan packedPlan) {
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(packedPlan);
  }

  @Override
  public void close() {
  }
}
