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
package com.twitter.heron.scheduler.utils;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * Interface of class that renders dry-run response
 */
public abstract class DryRunRender {
  protected Map<String, Resource> componentsResource(PackingPlan packingPlan) {
    Map<String, Resource> componentsResource = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan: packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan: containerPlan.getInstances()) {
        Resource resource = instancePlan.getResource();
        String componentName = instancePlan.getComponentName();
        Resource totalResource = componentsResource.get(componentName);
        if (totalResource == null) {
          componentsResource.put(componentName, resource);
        } else {
          componentsResource.replace(componentName, resource.plus(totalResource));
        }
      }
    }
    return componentsResource;
  }

  protected Map<String, Integer> componentsParallelism(PackingPlan packingPlan) {
    Map<String, Integer> componentsParallelism = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan: packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan: containerPlan.getInstances()) {
        String componentName = instancePlan.getComponentName();
        Integer parallelism = componentsParallelism.get(componentName);
        if (parallelism == null) {
          componentsParallelism.put(componentName, 1);
        } else {
          componentsParallelism.replace(componentName, 1 + parallelism);
        }
      }
    }
    return componentsParallelism;
  }

  public abstract String render(SubmitDryRunResponse resp);
  public abstract String render(UpdateDryRunResponse resp);
}
