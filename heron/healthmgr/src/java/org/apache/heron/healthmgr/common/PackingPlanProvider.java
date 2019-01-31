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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;

import org.apache.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.PackingPlan.ContainerPlan;
import org.apache.heron.spi.packing.PackingPlan.InstancePlan;
import org.apache.heron.spi.packing.PackingPlanProtoDeserializer;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import static org.apache.heron.healthmgr.HealthPolicyConfig.CONF_TOPOLOGY_NAME;

/**
 * A topology's packing plan may get updated after initial deployment. This provider is used to
 * fetch the latest version from the state manager and provide to any dependent components.
 */
public class PackingPlanProvider implements Provider<PackingPlan>, EventHandler<TopologyUpdate> {
  private static final Logger LOG = Logger.getLogger(PackingPlanProvider.class.getName());

  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  private PackingPlan packingPlan;

  @Inject
  public PackingPlanProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
                             EventManager eventManager,
                             @Named(CONF_TOPOLOGY_NAME) String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;
    eventManager.addEventListener(TopologyUpdate.class, this);
  }

  public String[] getBoltInstanceNames(String... boltComponents) {
    HashSet<String> boltComponentNames = new HashSet<>();
    Collections.addAll(boltComponentNames, boltComponents);

    PackingPlan packing = get();
    ArrayList<String> boltInstanceNames = new ArrayList<>();
    for (ContainerPlan containerPlan : packing.getContainers()) {
      for (InstancePlan instancePlan : containerPlan.getInstances()) {
        if (!boltComponentNames.contains(instancePlan.getComponentName())) {
          continue;
        }

        String name = "container_" + containerPlan.getId()
            + "_" + instancePlan.getComponentName()
            + "_" + instancePlan.getTaskId();
        boltInstanceNames.add(name);
      }
    }
    return boltInstanceNames.toArray(new String[boltInstanceNames.size()]);
  }

  @Override
  public synchronized PackingPlan get() {
    if (packingPlan == null) {
      fetchLatestPackingPlan();
    }

    return packingPlan;
  }

  /**
   * Invalidates cached packing plan on receiving update notification
   */
  @Override
  public synchronized void onEvent(TopologyUpdate event) {
    LOG.info("Received topology update event, invalidating cached PackingPlan: " + event);
    this.packingPlan = null;
  }

  private synchronized void fetchLatestPackingPlan() {
    PackingPlans.PackingPlan protoPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    this.packingPlan = deserializer.fromProto(protoPackingPlan);
    if (packingPlan == null) {
      throw new InvalidStateException(topologyName, "Failed to fetch the packing plan");
    }
  }
}
