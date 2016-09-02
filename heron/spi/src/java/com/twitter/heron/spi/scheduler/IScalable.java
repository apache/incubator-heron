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
package com.twitter.heron.spi.scheduler;

import java.util.Set;

import com.twitter.heron.classification.InterfaceAudience;
import com.twitter.heron.classification.InterfaceStability;
import com.twitter.heron.spi.packing.PackingPlan;

/**
 * Schedulers that implement {@code IScalable} support scaling topologies up or down by
 * adding or removing containers.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public interface IScalable {
  /**
   * Requests new containers for scaling a topology
   *
   * @param containersToAdd Set of containers to be added by the scheduler
   */
  void addContainers(Set<PackingPlan.ContainerPlan> containersToAdd);

  /**
   * Requests containers to be released for down-scaling a topology.
   *
   * @param containersToRemove Set of containers to be removed by the scheduler
   */
  void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove);
}
