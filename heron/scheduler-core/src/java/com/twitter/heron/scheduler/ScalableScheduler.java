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
package com.twitter.heron.scheduler;

import java.util.Map;

import com.twitter.heron.spi.packing.PackingPlan;

public interface ScalableScheduler {
  /**
   * Requests new containers for scaling a topology
   *
   * @param containers ContainerId to ContainerPlan map
   */
  void addContainers(Map<String, PackingPlan.ContainerPlan> containers);

  /**
   * Requests containers to be released for down-scaling a topology
   *
   * @param existingContainers Map of containers currently managed by the scheduler
   * @param containersToRemove ContainerId to ContainerPlan map
   */
  void removeContainers(Map<String, PackingPlan.ContainerPlan> existingContainers,
                        Map<String, PackingPlan.ContainerPlan> containersToRemove);
}
