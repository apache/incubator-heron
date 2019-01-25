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

package org.apache.heron.packing.builder;

import java.util.Objects;

import org.apache.heron.common.basics.ByteAmount;

/**
 * Helper class that captures the RAM requirements of each component
 */
public class ResourceRequirement implements Comparable<ResourceRequirement> {

  private String componentName;
  private ByteAmount ramRequirement;
  private double cpuRequirement;
  private SortingStrategy sortingStrategy;

  public ResourceRequirement(String componentName, ByteAmount ram) {
    this(componentName, ram, 0.0);
  }

  public ResourceRequirement(String componentName, ByteAmount ram, double cpu) {
    this(componentName, ram, cpu, SortingStrategy.RAM_FIRST);
  }

  public ResourceRequirement(String componentName,
                             ByteAmount ram,
                             double cpu,
                             SortingStrategy sortingStrategy) {
    this.componentName = componentName;
    this.ramRequirement = ram;
    this.cpuRequirement = cpu;
    this.sortingStrategy = sortingStrategy;
  }

  public String getComponentName() {
    return componentName;
  }

  @Override
  public int compareTo(ResourceRequirement other) {
    if (sortingStrategy == SortingStrategy.RAM_FIRST) {
      int ramComparison = this.ramRequirement.compareTo(other.ramRequirement);
      return ramComparison == 0
          ? Double.compare(this.cpuRequirement, other.cpuRequirement) : ramComparison;
    } else if (sortingStrategy == SortingStrategy.CPU_FIRST) {
      int cpuComparison = Double.compare(this.cpuRequirement, other.cpuRequirement);
      return cpuComparison == 0
          ? this.ramRequirement.compareTo(other.ramRequirement) : cpuComparison;
    } else {
      throw new IllegalStateException("Unknown SortingStrategy " + sortingStrategy.toString());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResourceRequirement)) {
      return false;
    }
    ResourceRequirement that = (ResourceRequirement) o;
    return Double.compare(that.cpuRequirement, cpuRequirement) == 0
        && ramRequirement.equals(that.ramRequirement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ramRequirement, cpuRequirement);
  }
}
