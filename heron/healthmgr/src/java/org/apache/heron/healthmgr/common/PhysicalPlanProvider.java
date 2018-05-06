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

package org.apache.heron.healthmgr.common;

import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;

import org.apache.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import org.apache.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import org.apache.heron.proto.system.PhysicalPlans.PhysicalPlan;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import static org.apache.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;

/**
 * A topology's physical plan may get updated after initial deployment. This provider is used to
 * fetch the latest version from the state manager and provide to any dependent components.
 */
public class PhysicalPlanProvider implements Provider<PhysicalPlan> {
  private static final Logger LOG = Logger.getLogger(PhysicalPlanProvider.class.getName());

  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  private PhysicalPlan physicalPlan;

  @Inject
  public PhysicalPlanProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
      EventManager eventManager, @Named(CONF_TOPOLOGY_NAME) String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;
    eventManager.addEventListener(TopologyUpdate.class, new EventHandler<TopologyUpdate>() {
      /**
       * Invalidates cached physical plan on receiving topology update notification
       */
      @Override
      public synchronized void onEvent(TopologyUpdate event) {
        LOG.info(
            "Received topology update event, invalidating cached PhysicalPlan: " + event.type());
        physicalPlan = null;
      }
    });
    eventManager.addEventListener(ContainerRestart.class, new EventHandler<ContainerRestart>() {
      /**
       * Invalidates cached physical plan on receiving container restart notification
       */
      @Override
      public synchronized void onEvent(ContainerRestart event) {
        LOG.info("Received container restart event, invalidating cached PhysicalPlan: "
            + event.type());
        physicalPlan = null;
      }
    });
  }

  @Override
  public synchronized PhysicalPlan get() {
    if (physicalPlan == null) {
      physicalPlan = stateManagerAdaptor.getPhysicalPlan(topologyName);
      if (physicalPlan == null) {
        throw new InvalidStateException(topologyName, "Failed to fetch the physical plan");
      }
    }
    return physicalPlan;
  }

}
