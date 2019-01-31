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

package org.apache.heron.metricscachemgr.metricscache.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ExceptionRequest {
  private final Map<String, Set<String>> componentNameInstanceId;

  public ExceptionRequest(Map<String, Set<String>> componentNameInstanceId) {
    if (componentNameInstanceId == null) {
      this.componentNameInstanceId = null;
    } else {
      Map<String, Set<String>> map = new HashMap<>();
      for (String componentName : componentNameInstanceId.keySet()) {
        Set<String> instanceId = componentNameInstanceId.get(componentName);
        if (instanceId == null) {
          map.put(componentName, null);
        } else {
          Set<String> set = new HashSet<>(instanceId);
          map.put(componentName, set);
        }
      }
      this.componentNameInstanceId = map;
    }
  }

  public Map<String, Set<String>> getComponentNameInstanceId() {
    Map<String, Set<String>> ret;
    if (componentNameInstanceId == null) {
      ret = null;
    } else {
      Map<String, Set<String>> map = new HashMap<>();
      for (String componentName : componentNameInstanceId.keySet()) {
        Set<String> instanceId = componentNameInstanceId.get(componentName);
        if (instanceId == null) {
          map.put(componentName, null);
        } else {
          Set<String> set = new HashSet<>(instanceId);
          map.put(componentName, set);
        }
      }
      ret = map;
    }
    return ret;
  }
}
