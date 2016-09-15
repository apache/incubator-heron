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


package com.twitter.heron.packing;

import java.util.ArrayList;

import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.Resource;

/**
 * Class that describes a container used to place Heron Instances with specific memory, CpuCores and disk
 * requirements. Each container has limited ram, CpuCores and disk resources.
 */
public class Container {

  private ArrayList<InstanceId> instanceIds;

  //Resources currently used by the container.
  private long usedRam;
  private double usedCpuCores;
  private long usedDisk;

  //Maximum resources that can be assigned to the container.
  private long maxRam;
  private double maxCpuCores;
  private long maxDisk;

  public Container(Resource resource) {
    this.usedRam = 0;
    this.usedCpuCores = 0;
    this.usedDisk = 0;
    this.maxRam = resource.getRam();
    this.maxCpuCores = resource.getCpu();
    this.maxDisk = resource.getDisk();
    instanceIds = new ArrayList<InstanceId>();
  }

  /**
   * Check whether the container can accommodate a new instance with specific resource requirements
   *
   * @return true if the container has space otherwise return false
   */
  private boolean hasSpace(long ram, double cpuCores, long disk) {
    return usedRam + ram <= maxRam
        && usedCpuCores + cpuCores <= maxCpuCores
        && usedDisk + disk <= maxDisk;
  }

  /**
   * Update the resources currently used by the container, when a new instance with specific
   * resource requirements has been assigned to the container.
   *
   * @return true if the instance can be added to the container, false otherwise
   */
  public boolean add(Resource resource, InstanceId instanceId) {
    if (this.hasSpace(resource.getRam(), resource.getCpu(), resource.getDisk())) {
      usedRam += resource.getRam();
      usedCpuCores += resource.getCpu();
      usedDisk += resource.getDisk();
      instanceIds.add(instanceId);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Remove an instance of a particular component from a container and update its
   * corresponding resources.
   *
   * @return instanceId if the instance is removed the container.
   * Return null if the container is empty or
   * does not contain an instance of the given component.
   */
  public InstanceId remove(Resource resource, String component) {
    if (!instanceIds.isEmpty()) {
      InstanceId instanceId = instanceOfComponent(component);
      if (instanceId != null) {
        usedRam -= resource.getRam();
        usedCpuCores -= resource.getCpu();
        usedDisk -= resource.getDisk();
        instanceIds.remove(instanceId);
        return instanceId;
      } else {
        return null;
      }
    }
    return null;
  }

  /**
   * Find whether an instance of a particular component is assigned to the container
   *
   * @return the instanceId if an instance is found, null otherwise
   */
  public InstanceId instanceOfComponent(String component) {
    for (int i = 0; i < instanceIds.size(); i++) {
      if (instanceIds.get(i).getComponentName().equals(component)) {
        return instanceIds.get(i);
      }
    }
    return null;
  }
}
