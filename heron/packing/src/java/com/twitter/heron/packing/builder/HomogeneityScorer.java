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
package com.twitter.heron.packing.builder;

import com.twitter.heron.spi.packing.PackingPlan;

/**
 * Scores based on how homogeneous a container is relative to a given component, from highest to
 * lowest. So a container with all the same components (e.g., AAAA) will get the highest score for
 * A, while a container with a mix of many components (e.g. ABCD) will get a low score.
 *
 * If run in binary mode, score is a 1 if all instances on the container are of componentName,
 * and 0 otherwise
 */
public class HomogeneityScorer implements Scorer<Container> {
  private String componentName;
  private boolean binaryMode;

  public HomogeneityScorer(String componentName, boolean binaryMode) {
    this.componentName = componentName;
    this.binaryMode = binaryMode;
  }

  @Override
  public boolean sortAscending() {
    return false;
  }

  @Override
  public double getScore(Container container) {
    int totalComponentInstances = 0;
    int totalInstances = container.getInstances().size();

    if (totalInstances == 0) {
      return 0;
    }

    for (PackingPlan.InstancePlan instancePlan : container.getInstances()) {
      if (componentName.equals(instancePlan.getComponentName())) {
        totalComponentInstances++;
      }
    }
    if (binaryMode) {
      if (totalComponentInstances == totalInstances) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return (double) totalComponentInstances / totalInstances;
    }
  }
}
