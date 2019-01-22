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

package org.apache.heron.spi.packing;

/**
 * Encapsulates the unique components of an Instance identifier.
 */
public final class InstanceId {

  private final String componentName;
  private final int taskId;
  private final int componentIndex;

  public InstanceId(String componentName, int taskId, int componentIndex) {
    this.componentName = componentName;
    this.taskId = taskId;
    this.componentIndex = componentIndex;
  }

  public String getComponentName() {
    return componentName;
  }

  public int getTaskId() {
    return taskId;
  }

  public int getComponentIndex() {
    return componentIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InstanceId that = (InstanceId) o;

    return taskId == that.taskId
        && componentIndex == that.componentIndex
        && componentName.equals(that.componentName);
  }

  @Override
  public int hashCode() {
    int result = componentName.hashCode();
    result = 31 * result + taskId;
    result = 31 * result + componentIndex;
    return result;
  }

  @Override
  public String toString() {
    return String.format("{componentName: %s, taskId: %d, componentIndex: %d}",
        componentName, taskId, componentIndex);
  }
}
