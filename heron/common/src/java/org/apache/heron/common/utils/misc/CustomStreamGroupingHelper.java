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

package org.apache.heron.common.utils.misc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.heron.api.grouping.CustomStreamGrouping;
import org.apache.heron.api.topology.TopologyContext;

class CustomStreamGroupingHelper {
  // Mapping from steamid to a List of Targets
  private final Map<String, List<Target>> targets;

  CustomStreamGroupingHelper() {
    targets = new HashMap<>();
  }

  public void add(String streamId,
                  List<Integer> taskIds,
                  CustomStreamGrouping grouping,
                  String sourceComponentName) {
    if (!targets.containsKey(streamId)) {
      targets.put(streamId, new ArrayList<Target>());
    }
    targets.get(streamId).add(new Target(taskIds, grouping, sourceComponentName));
  }

  void prepare(TopologyContext context) {
    for (String streamId : targets.keySet()) {
      for (Target target : targets.get(streamId)) {
        target.prepare(context, streamId);
      }
    }
  }

  List<Integer> chooseTasks(String streamId, List<Object> values) {
    List<Target> targetList = targets.get(streamId);
    if (targetList != null) {
      List<Integer> res = new ArrayList<>();
      for (Target target : targetList) {
        res.addAll(target.chooseTasks(values));
      }

      return res;
    }
    return null;
  }

  boolean isCustomGroupingEmpty() {
    return targets.isEmpty();
  }

  private static class Target {
    private final String componentName;
    private final List<Integer> taskIds;
    private final CustomStreamGrouping grouping;

    Target(List<Integer> taskIds, CustomStreamGrouping grouping, String componentName) {
      this.taskIds = taskIds;
      this.grouping = grouping;
      this.componentName = componentName;
    }

    public String getComponentName() {
      return componentName;
    }

    public List<Integer> getTaskIds() {
      return taskIds;
    }

    public CustomStreamGrouping getGrouping() {
      return grouping;
    }

    public void prepare(TopologyContext context, String streamId) {
      grouping.prepare(context, componentName, streamId, taskIds);
    }

    private List<Integer> chooseTasks(List<Object> values) {
      return grouping.chooseTasks(values);
    }
  }
}

