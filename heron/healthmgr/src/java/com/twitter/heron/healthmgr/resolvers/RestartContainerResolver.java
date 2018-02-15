// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.healthmgr.resolvers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.core.Action;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

import com.twitter.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import com.twitter.heron.healthmgr.common.PhysicalPlanProvider;
import com.twitter.heron.proto.scheduler.Scheduler.RestartTopologyRequest;
import com.twitter.heron.scheduler.client.ISchedulerClient;

import static com.twitter.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;

public class RestartContainerResolver implements IResolver {
  private static final Logger LOG = Logger.getLogger(RestartContainerResolver.class.getName());

  private final PhysicalPlanProvider physicalPlanProvider;
  private final EventManager eventManager;
  private final String topologyName;
  private final ISchedulerClient schedulerClient;
  private ExecutionContext context;

  @Inject
  public RestartContainerResolver(@Named(CONF_TOPOLOGY_NAME) String topologyName,
                                  PhysicalPlanProvider physicalPlanProvider,
                                  EventManager eventManager,
                                  ISchedulerClient schedulerClient) {
    this.topologyName = topologyName;
    this.physicalPlanProvider = physicalPlanProvider;
    this.eventManager = eventManager;
    this.schedulerClient = schedulerClient;
  }

  @Override
  public void initialize(ExecutionContext context) {
    this.context = context;
  }

  @Override
  public Collection<Action> resolve(Collection<Diagnosis> diagnosis) {
    List<Action> actions = new ArrayList<>();

    // find all back pressure measurements reported in this execution cycle
    Instant current = context.checkpoint();
    Instant previous = context.previousCheckpoint();
    MeasurementsTable bpMeasurements = context.measurements()
        .type(METRIC_BACK_PRESSURE.text())
        .between(previous, current);

    if (bpMeasurements.size() == 0) {
      LOG.fine("No back-pressure measurements found, ending as there's nothing to fix");
      return actions;
    }

    Collection<String> allBpInstances = bpMeasurements.uniqueInstances();

    // find instance with the highest total back pressure
    MeasurementsTable maxBpTable = null;
    for (String bpInstance : allBpInstances) {
      MeasurementsTable instanceTable = bpMeasurements.instance(bpInstance);
      if (maxBpTable == null || maxBpTable.sum() < instanceTable.sum()) {
        maxBpTable = instanceTable;
      }
    }

    // want to know which stream manager starts back-pressure
    String instanceId = maxBpTable.first().instance();
    int fromIndex = instanceId.indexOf('_') + 1;
    int toIndex = instanceId.indexOf('_', fromIndex);
    String stmgrId = instanceId.substring(fromIndex, toIndex);

    LOG.info("Restarting container: " + stmgrId);
    boolean b = schedulerClient.restartTopology(
        RestartTopologyRequest.newBuilder()
            .setContainerIndex(Integer.valueOf(stmgrId))
            .setTopologyName(topologyName)
            .build());
    LOG.info("Restarted container result: " + b);

    LOG.info("Broadcasting container restart event");
    ContainerRestart action = new ContainerRestart(current, Collections.singletonList(instanceId));
    eventManager.onEvent(action);
    actions.add(action);
    return actions;
  }
}
