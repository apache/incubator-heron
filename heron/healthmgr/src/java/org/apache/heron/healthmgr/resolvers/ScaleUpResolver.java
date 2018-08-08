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

package org.apache.heron.healthmgr.resolvers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.core.Action;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.DiagnosisTable;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

import org.apache.heron.api.generated.TopologyAPI.Topology;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import org.apache.heron.healthmgr.common.PackingPlanProvider;
import org.apache.heron.healthmgr.common.PhysicalPlanProvider;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.scheduler.client.ISchedulerClient;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.IRepacking;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.PackingPlanProtoSerializer;
import org.apache.heron.spi.utils.ReflectionUtils;

import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_UNDER_PROVISIONING;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;


public class ScaleUpResolver implements IResolver {
  private static final Logger LOG = Logger.getLogger(ScaleUpResolver.class.getName());

  private PhysicalPlanProvider physicalPlanProvider;
  private PackingPlanProvider packingPlanProvider;
  private ISchedulerClient schedulerClient;
  private EventManager eventManager;
  private Config config;
  private ExecutionContext context;

  @Inject
  public ScaleUpResolver(PhysicalPlanProvider physicalPlanProvider,
                         PackingPlanProvider packingPlanProvider,
                         ISchedulerClient schedulerClient,
                         EventManager eventManager,
                         Config config) {
    this.physicalPlanProvider = physicalPlanProvider;
    this.packingPlanProvider = packingPlanProvider;
    this.schedulerClient = schedulerClient;
    this.eventManager = eventManager;
    this.config = config;
  }

  @Override
  public void initialize(ExecutionContext ctxt) {
    this.context = ctxt;
  }

  @Override
  public Collection<Action> resolve(Collection<Diagnosis> diagnosis) {
    List<Action> actions = new ArrayList<>();

    DiagnosisTable table = DiagnosisTable.of(diagnosis);
    table = table.type(DIAGNOSIS_UNDER_PROVISIONING.text());

    if (table.size() == 0) {
      LOG.fine("No under-previsioning diagnosis present, ending as there's nothing to fix");
      return actions;
    }

    // Scale the first assigned component
    Diagnosis diagnoses = table.first();
    // verify diagnoses instance is valid
    if (diagnoses.assignments().isEmpty()) {
      LOG.warning(String.format("Diagnosis %s is missing assignments", diagnoses.id()));
      return actions;
    }
    String component = diagnoses.assignments().iterator().next();

    int newParallelism = computeScaleUpFactor(component);
    Map<String, Integer> changeRequest = new HashMap<>();
    changeRequest.put(component, newParallelism);

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
      throw new RuntimeException(String.format("Failed to update topology with Scheduler, "
          + "updateTopologyRequest=%s", updateTopologyRequest));
    }
    LOG.info("Scheduler updated topology successfully.");

    LOG.info("Broadcasting topology update event");
    TopologyUpdate action
        = new TopologyUpdate(context.checkpoint(), Collections.singletonList(component));
    eventManager.onEvent(action);

    actions.add(action);
    return actions;
  }

  @VisibleForTesting
  int computeScaleUpFactor(String compName) {
    Instant newest = context.checkpoint();
    Instant oldest = context.previousCheckpoint();
    MeasurementsTable table = context.measurements()
        .component(compName)
        .type(METRIC_BACK_PRESSURE.text())
        .between(oldest, newest);

    double totalCompBpTime = table.sum();
    LOG.info(String.format("Component: %s, bpTime: %.0f", compName, totalCompBpTime));

    if (totalCompBpTime >= 1000) {
      totalCompBpTime = 999;
      LOG.warning(String.format("Comp:%s, bpTime after filter: %.0f", compName, totalCompBpTime));
    }

    double unusedCapacity = (1.0 * totalCompBpTime) / (1000 - totalCompBpTime);

    // scale up fencing: do not scale more than 4 times the current size
    unusedCapacity = unusedCapacity > 4.0 ? 4.0 : unusedCapacity;
    int parallelism = (int) Math.ceil(table.uniqueInstances().size() * (1 + unusedCapacity));
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

    Topology topology = physicalPlanProvider.get().getTopology();
    try {
      packing.initialize(config, topology);
      return packing.repack(currentPackingPlan, componentDeltas);
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

  private PackingPlans.PackingPlan getSerializedPlan(PackingPlan packedPlan) {
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(packedPlan);
  }

  @Override
  public void close() {
  }
}
