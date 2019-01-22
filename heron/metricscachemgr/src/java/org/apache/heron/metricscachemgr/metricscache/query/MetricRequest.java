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

/**
 * immutable data bag for metric request
 * equality: &lt;componentName, instanceId, metricName&gt;
 * range: startTime ~ endTime
 * type: aggregationGranularity
 */
public final class MetricRequest {
  // The instance ids to get the stats from
  // If nothing is specified, we will get from
  // all the instances of the component name
  private final Map<String, Set<String>> componentNameInstanceId;
  // What set of metrics you are interested in
  // Example is __emit-count/default
  private final Set<String> metricNames;
  // what timeframe data in milliseconds
  private final long startTime;
  private final long endTime;
  private final MetricGranularity aggregationGranularity;

  public MetricRequest(Map<String, Set<String>> componentNameInstanceId,
                       Set<String> metricNames,
                       long startTime, long endTime,
                       MetricGranularity aggregationGranularity) {
    if (componentNameInstanceId == null) {
      this.componentNameInstanceId = null;
    } else {
      this.componentNameInstanceId = new HashMap<>(componentNameInstanceId);
    }
    if (metricNames == null) {
      this.metricNames = null;
    } else {
      this.metricNames = new HashSet<>(metricNames);
    }
    this.startTime = startTime;
    this.endTime = endTime;
    this.aggregationGranularity = aggregationGranularity;
  }

  public Map<String, Set<String>> getComponentNameInstanceId() {
    if (componentNameInstanceId == null) {
      return null;
    }
    Map<String, Set<String>> ret = new HashMap<>();
    for (String key : componentNameInstanceId.keySet()) {
      Set<String> value = null;
      if (componentNameInstanceId.get(key) != null) {
        value = new HashSet<>();
        value.addAll(componentNameInstanceId.get(key));
        ret.put(key, value);
      }
      ret.put(key, value);
    }
    return ret;
  }

  public Set<String> getMetricNames() {
    if (metricNames == null) {
      return null;
    }
    return new HashSet<>(metricNames);
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public MetricGranularity getAggregationGranularity() {
    return aggregationGranularity;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{")
        .append("[").append(startTime).append("-").append(endTime)
        .append(":").append(aggregationGranularity).append("]")
        .append("[");
    if (componentNameInstanceId != null) {
      for (String c : componentNameInstanceId.keySet()) {
        sb.append(c).append("->(");
        if (componentNameInstanceId.get(c) == null) {
          sb.append("null");
        } else {
          for (String i : componentNameInstanceId.get(c)) {
            sb.append(i).append(",");
          }
        }
        sb.append("),");
      }
    }
    sb.append("]")
        .append("[");
    if (metricNames != null) {
      for (String name : metricNames) {
        sb.append(name).append(",");
      }
    }
    sb.append("]")
        .append("}");
    return sb.toString();
  }
}
