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

import javax.inject.Provider;
import javax.inject.Singleton;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.ContainerPlan;
import com.twitter.heron.spi.packing.PackingPlan.InstancePlan;

/**
 * A topology's packing plan may get updated after initial deployment. This provider is used to
 * fetch the latest version from the state manager and provide to any dependent components.
 */
@Singleton
public class PackingPlanProvider implements Provider<PackingPlan> {
  private PackingPlan packingPlan;

  public String[] getBoltInstanceNames(String... boltComponents) {
    HashSet<String> boltComponentNames = new HashSet<>();
    for (String boltComponentName : boltComponents) {
      boltComponentNames.add(boltComponentName);
    }

    ArrayList<String> boltInstanceNames = new ArrayList<>();
    for (ContainerPlan containerPlan : packingPlan.getContainers()) {
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
  public PackingPlan get() {
    return packingPlan;
  }

  public void setPackingPlan(PackingPlan packingPlan) {
    this.packingPlan = packingPlan;
  }
}