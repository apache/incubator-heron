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

import org.apache.heron.packing.exceptions.MinResourceNotSatisfiedException;
import org.apache.heron.spi.packing.PackingPlan;

public class MinCpuConstraint implements InstanceConstraint {
  private static final double MIN_CPU_PER_INSTANCE = 0.1;

  @Override
  public void validate(PackingPlan.InstancePlan instancePlan)
      throws MinResourceNotSatisfiedException {
    if (instancePlan.getResource().getCpu() < MIN_CPU_PER_INSTANCE) {
      throw new MinResourceNotSatisfiedException(String.format("Instance %s is "
          + "configured %.3f CPU that is less than the minimum CPU per instance %.3f",
          instancePlan.getComponentName(), instancePlan.getResource().getCpu(),
          MIN_CPU_PER_INSTANCE));
    }
  }
}
