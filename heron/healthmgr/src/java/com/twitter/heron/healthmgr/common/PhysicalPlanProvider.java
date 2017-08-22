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

package com.twitter.heron.healthmgr.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans.PhysicalPlan;
import com.twitter.heron.proto.system.PhysicalPlans.StMgr;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import static com.twitter.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;

/**
 * A topology's physical plan may get updated after initial deployment. This provider is used to
 * fetch the latest version from the state manager and provide to any dependent components.
 */
public class PhysicalPlanProvider implements Provider<PhysicalPlan>, EventHandler<TopologyUpdate> {
  private static final Logger LOG = Logger.getLogger(PhysicalPlanProvider.class.getName());

  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  private PhysicalPlan physicalPlan;

  @Inject
  public PhysicalPlanProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
      EventManager eventManager, @Named(CONF_TOPOLOGY_NAME) String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;
    eventManager.addEventListener(TopologyUpdate.class, this);
  }

  public String getShellUrl(String stmgrId) {
    if (physicalPlan != null) {
      for (StMgr stmgr : physicalPlan.getStmgrsList()) {
        if (stmgr.getId().equals(stmgrId)) {
          return stmgr.getHostName() + stmgr.getShellPort();
        }
      }
    }
    return null;
  }

  @Override
  public synchronized PhysicalPlan get() {
    if (physicalPlan == null) {
      fetchLatestPhysicalPlan();
    }
    return physicalPlan;
  }

  /**
   * Invalidates cached physical plan on receiving update notification
   */
  @Override
  public synchronized void onEvent(TopologyUpdate event) {
    LOG.info("Received topology update event, invalidating cached PhysicalPlan: " + event);
    physicalPlan = null;
  }

  /**
   * Invalidates cached physical plan on receiving container restart notification
   */
  @Override
  public synchronized void onEvent(ContainerRestart event) {
    LOG.info("Received conatiner restart event, invalidating cached PhysicalPlan: " + event);
    physicalPlan = null;
  }

  private synchronized void fetchLatestPhysicalPlan() {
    physicalPlan = stateManagerAdaptor.getPhysicalPlan(topologyName);
    if (physicalPlan == null) {
      throw new InvalidStateException(topologyName, "Failed to fetch the physical plan");
    }
  }
}
