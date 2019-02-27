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

package org.apache.heron.packing.constraints;

import org.apache.heron.packing.builder.Container;
import org.apache.heron.packing.exceptions.ResourceExceededException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

public class ResourceConstraint implements PackingConstraint {
  @Override
  public void validate(Container container, PackingPlan.InstancePlan instancePlan)
      throws ResourceExceededException {
    Resource usedResource = container.getTotalUsedResources();
    Resource newUsedResource = usedResource.plus(instancePlan.getResource());
    Resource capacity = container.getCapacity();
    if (capacity.getCpu() < newUsedResource.getCpu()) {
      throw new ResourceExceededException(String.format("Adding instance %s with %.3f cores "
              + "to container %d with existing %.3f cores "
              + "would exceed its capacity of %.3f cores",
          instancePlan.getComponentName(),
          instancePlan.getResource().getCpu(),
          container.getContainerId(),
          usedResource.getCpu(),
          capacity.getCpu()));
    }
    if (capacity.getRam().lessThan(newUsedResource.getRam())) {
      throw new ResourceExceededException(String.format("Adding instance %s with %s RAM "
              + "to container %d with existing %s RAM "
              + "would exceed its capacity of %s RAM",
          instancePlan.getComponentName(),
          instancePlan.getResource().getRam().toString(),
          container.getContainerId(),
          usedResource.getRam().toString(),
          capacity.getRam().toString()));
    }
    if (capacity.getDisk().lessThan(newUsedResource.getDisk())) {
      throw new ResourceExceededException(String.format("Adding instance %s with %s disk "
              + "to container %d with existing %s disk "
              + "would exceed its capacity of %s disk",
          instancePlan.getComponentName(),
          instancePlan.getResource().getDisk().toString(),
          container.getContainerId(),
          usedResource.getDisk().toString(),
          capacity.getDisk().toString()));
    }
  }
}
