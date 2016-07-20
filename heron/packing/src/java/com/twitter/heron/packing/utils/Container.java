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

package com.twitter.heron.packing.utils;

/**
 * Class that describes a container used to place Heron Instances with specific memory, CpuCores and disk
 * requirements. Each container has limited ram, CpuCores and disk resources.
 */
public class Container {

  //Resources currently used by the container.
  private long usedRam;
  private double usedCpuCores;
  private long usedDisk;

  //Maximum resources that can be assigned to the container.
  private long maxRam;
  private double maxCpuCores;
  private long maxDisk;

  public Container(long maxRam, double maxCpuCores, long maxDisk) {
    this.usedRam = 0;
    this.usedCpuCores = 0;
    this.usedDisk = 0;
    this.maxRam = maxRam;
    this.maxCpuCores = maxCpuCores;
    this.maxDisk = maxDisk;
  }

  /**
   * Check whether the container can accommodate a new instance with specific resource requirements
   *
   * @return true if the container has space otherwise return false
   */
  public boolean hasSpace(long ram, double cpuCores, long disk) {
    if (usedRam + ram <= maxRam
        && usedCpuCores + cpuCores <= maxCpuCores
        && usedDisk + disk <= maxDisk) {

      return true;
    }
    return false;
  }

  /**
   * Update the resources currently used by the container, when a new instance with specific
   * resource requirements has been assigned to the container.
   *
   * @return true if the instance can be added to the container, false otherwise
   */
  public boolean add(long ram, double cpuCores, long disk) {
    if (this.hasSpace(ram, cpuCores, disk)) {
      usedRam += ram;
      usedCpuCores += cpuCores;
      usedDisk += disk;
      return true;
    } else {
      return false;
    }
  }


}
