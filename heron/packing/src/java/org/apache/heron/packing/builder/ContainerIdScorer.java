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

/**
 * Sorts containers ascending by container id. If firstId and maxId as passed, start with
 * container with id firstId and ends with id firstId - 1, looping from max id back to lowest id.
 */
public class ContainerIdScorer implements Scorer<Container> {

  private final boolean sortAscending;
  private final Integer firstId;
  private final Integer maxId;

  public ContainerIdScorer() {
    this(true);
  }

  public ContainerIdScorer(boolean sortAscending) {
    this(0, 0, sortAscending);
  }

  public ContainerIdScorer(Integer firstId, Integer maxId) {
    this(firstId, maxId, true);
  }

  private ContainerIdScorer(Integer firstId, Integer maxId, boolean sortAscending) {
    this.sortAscending = sortAscending;
    this.firstId = firstId;
    this.maxId = maxId;
  }

  @Override
  public boolean sortAscending() {
    return sortAscending;
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
