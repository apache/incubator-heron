// Copyright 2016 Microsoft. All rights reserved.
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

package com.twitter.heron.healthmgr.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.twitter.heron.healthmgr.common.HealthManagerEvents.TOPOLOGY_UPDATE;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.ContainerPlan;
import com.twitter.heron.spi.packing.PackingPlan.InstancePlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

/**
 * A topology's packing plan may get updated after initial deployment. This provider is used to
 * fetch the latest version from the state manager and provide to any dependent components.
 */
@Singleton
public class PackingPlanProvider implements Provider<PackingPlan> {
  private static final Logger LOG = Logger.getLogger(PackingPlanProvider.class.getName());

  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  private PackingPlan packingPlan;

  @Inject
  public PackingPlanProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
                             String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;
  }

  public String[] getBoltInstanceNames(String... boltComponents) {
    HashSet<String> boltComponentNames = new HashSet<>();
    for (String boltComponentName : boltComponents) {
      boltComponentNames.add(boltComponentName);
    }

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
  public synchronized void onNext(TOPOLOGY_UPDATE event) {
    LOG.info("Received topology update event, invalidating cached PackingPlan: " + event);
    this.packingPlan = null;
  }

  private synchronized void fetchLatestPackingPlan() {
    PackingPlans.PackingPlan protoPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    this.packingPlan = deserializer.fromProto(protoPackingPlan);
  }
}