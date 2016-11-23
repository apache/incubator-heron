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

/**
 * Sorts containers ascending by container id. Id firstId and maxId as passed, start with
 * container with id firstId and ends with id firstId - 1
 */
public class ContainerIdScorer implements Scorer<Container> {

  private Integer firstId;
  private Integer maxId;

  public ContainerIdScorer() {
    this(0, 0);
  }

  public ContainerIdScorer(Integer firstId, Integer maxId) {
    this.firstId = firstId;
    this.maxId = maxId;
  }

  @Override
  public boolean sortAscending() {
    return true;
  }

  @Override
  public double getScore(Container container) {
    int containerId = container.getContainerId();
    if (containerId >= firstId) {
      return containerId - firstId;
    } else {
      return containerId + maxId;
    }
  }
}
