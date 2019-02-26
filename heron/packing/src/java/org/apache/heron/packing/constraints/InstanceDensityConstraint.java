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
import org.apache.heron.packing.exceptions.TooManyInstancesException;
import org.apache.heron.spi.packing.PackingPlan;

/**
 * Constraint on the number of instances in one container
 */
public class InstanceDensityConstraint implements PackingConstraint {
  private final int maxNumInstancesPerContainer;

  public InstanceDensityConstraint(int maxNumInstancesPerContainer) {
    this.maxNumInstancesPerContainer = maxNumInstancesPerContainer;
  }

  @Override
  public void validate(Container container, PackingPlan.InstancePlan instancePlan)
      throws TooManyInstancesException {
    if (container.getInstances().size() + 1 > maxNumInstancesPerContainer) {
      throw new TooManyInstancesException(String.format("Adding instance %s to container %d "
              + "will cause the container to have more instances "
              + "than the configured maxNumInstancesPerContainer %d",
          instancePlan.getComponentName(),
          container.getContainerId(),
          maxNumInstancesPerContainer));
    }
  }
}
