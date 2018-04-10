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
package org.apache.heron.healthmgr.resolvers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.core.Action;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.SymptomsTable;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

import org.apache.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import org.apache.heron.proto.scheduler.Scheduler.RestartTopologyRequest;
import org.apache.heron.scheduler.client.ISchedulerClient;

import static org.apache.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_INSTANCE_BACK_PRESSURE;

public class RestartContainerResolver implements IResolver {
  private static final Logger LOG = Logger.getLogger(RestartContainerResolver.class.getName());

  private final EventManager eventManager;
  private final String topologyName;
  private final ISchedulerClient schedulerClient;
  private ExecutionContext context;

  @Inject
  public RestartContainerResolver(@Named(CONF_TOPOLOGY_NAME) String topologyName,
                                  EventManager eventManager,
                                  ISchedulerClient schedulerClient) {
    this.topologyName = topologyName;
    this.eventManager = eventManager;
    this.schedulerClient = schedulerClient;
  }

  @Override
  public void initialize(ExecutionContext ctxt) {
    this.context = ctxt;
  }

  @Override
  public Collection<Action> resolve(Collection<Diagnosis> diagnosis) {
    List<Action> actions = new ArrayList<>();

    // find all back pressure measurements reported in this execution cycle
    Instant current = context.checkpoint();
    Instant previous = context.previousCheckpoint();
    SymptomsTable bpSymptoms = context.symptoms()
        .type(SYMPTOM_INSTANCE_BACK_PRESSURE.text())
        .between(previous, current);

    if (bpSymptoms.size() == 0) {
      LOG.fine("No back-pressure measurements found, ending as there's nothing to fix");
      return actions;
    }

    Collection<String> allBpInstances = new HashSet<>();
    bpSymptoms.get().forEach(symptom -> allBpInstances.addAll(symptom.assignments()));

    LOG.info(String.format("%d instances caused back-pressure", allBpInstances.size()));

    Collection<String> stmgrIds = new HashSet<>();
    allBpInstances.forEach(instanceId -> {
      LOG.info("Id of instance causing back-pressure: " + instanceId);
      int fromIndex = instanceId.indexOf('_') + 1;
      int toIndex = instanceId.indexOf('_', fromIndex);
      String stmgrId = instanceId.substring(fromIndex, toIndex);
      stmgrIds.add(stmgrId);
    });

    stmgrIds.forEach(stmgrId -> {
      LOG.info("Restarting container: " + stmgrId);
      boolean b = schedulerClient.restartTopology(
          RestartTopologyRequest.newBuilder()
              .setContainerIndex(Integer.valueOf(stmgrId))
              .setTopologyName(topologyName)
              .build());
      LOG.info("Restarted container result: " + b);
    });

    LOG.info("Broadcasting container restart event");
    ContainerRestart action = new ContainerRestart(current, stmgrIds);
    eventManager.onEvent(action);
    actions.add(action);
    return actions;
  }
}
