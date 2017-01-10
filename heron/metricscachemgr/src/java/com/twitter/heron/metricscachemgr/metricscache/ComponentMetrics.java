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
package com.twitter.heron.metricscachemgr.metricscache;


import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter.MetricAggregationType;

// Component level metrics. A component metrics is a map storing metrics for each of its
// instance as 'InstanceMetrics'.
public class ComponentMetrics {
  private static final Logger LOG = Logger.getLogger(ComponentMetrics.class.getName());

  private String componentName;
  private int nbuckets;
  private int bucketInterval;
  // map between instance id and its set of metrics
  private Map<String, InstanceMetrics> metricsInstance;

  // ctor. '_componentName' is the user supplied name given to the spout/bolt. '_nbuckets' is
  // number of buckets stored for this component.
  public ComponentMetrics(String componentName, int nbuckets, int bucketInterval) {
    this.componentName = componentName;
    this.nbuckets = nbuckets;
    this.bucketInterval = bucketInterval;

    metricsInstance = new HashMap<>();
  }

  // Remove old metrics and exception associated with this spout/bolt component.
  public void Purge() {
    for (InstanceMetrics im : metricsInstance.values()) {
      im.Purge();
    }
  }

  // Add metrics for an Instance 'instanceId' of this spout/bolt component.
  public void AddMetricForInstance(String instanceId, String name,
                                   MetricAggregationType type, String value) {
    InstanceMetrics instanceMetrics = GetOrCreateInstanceMetrics(instanceId);
    LOG.info("AddMetricForInstance " + instanceId
        + "; name " + name + "; type " + type + "; value " + value);
    instanceMetrics.AddMetricWithName(name, type, value);
  }

  // Request aggregated metrics for this component for the '_nbucket' interval.
  // Doesn't own '_response' object.
  public void GetMetrics(TopologyMaster.MetricRequest request, long startTime, long endTime,
                         TopologyMaster.MetricResponse.Builder responseBuilder) {
    if (request.getInstanceIdCount() == 0) {
      // This means that all instances need to be returned
      for (InstanceMetrics im : metricsInstance.values()) {
        im.GetMetrics(request, startTime, endTime, responseBuilder);
        if (responseBuilder.getStatus().getStatus() != Common.StatusCode.OK) {
          return;
        }
      }
    } else {
      for (int i = 0; i < request.getInstanceIdCount(); ++i) {
        String id = request.getInstanceId(i);
        if (!metricsInstance.containsKey(id)) {
          LOG.log(Level.SEVERE, "GetMetrics request received for unknown instanceId " + id);
          responseBuilder.setStatus(responseBuilder.getStatusBuilder()
              .setStatus(Common.StatusCode.NOTOK).build());
          return;
        } else {
          metricsInstance.get(id).GetMetrics(request, startTime, endTime, responseBuilder);
          if (responseBuilder.getStatus().getStatus() != Common.StatusCode.OK) {
            return;
          }
        }
      }
    }
    responseBuilder.setStatus(responseBuilder.getStatusBuilder()
        .setStatus(Common.StatusCode.OK).build());
  }


  // Create or return existing mutable InstanceMetrics associated with 'instanceId'. This
  // method doesn't verify if the instanceId is valid fof the component.
  // Doesn't transfer ownership of returned InstanceMetrics.
  private InstanceMetrics GetOrCreateInstanceMetrics(String instanceId) {
    if (!metricsInstance.containsKey(instanceId)) {
      metricsInstance.put(instanceId, new InstanceMetrics(instanceId, nbuckets, bucketInterval));
    }
    return metricsInstance.get(instanceId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("component name: ").append(componentName)
        .append("; nbuckets: ").append(nbuckets)
        .append("; bucketInterval: ").append(bucketInterval)
        .append("; metrics_instance:");
    for (String k : metricsInstance.keySet()) {
      sb.append("\n ").append(k).append(" ~> ").append(metricsInstance.get(k).toString());
    }
    return sb.toString();
  }

  public void GetMetrics(MetricsCacheQueryUtils.MetricCacheRequest request, long startTime, long endTime,
                         MetricsCacheQueryUtils.MetricCacheResponse response) {
    if (request.instanceId.isEmpty()) {
      // This means that all instances need to be returned
      for (InstanceMetrics im : metricsInstance.values()) {
        im.GetMetrics(request, startTime, endTime, response);
        if (response.status.status != 1) {
          return;
        }
      }
    } else {
      for (int i = 0; i < request.instanceId.size(); ++i) {
        String id = request.instanceId.get(i);
        if (!metricsInstance.containsKey(id)) {
          LOG.log(Level.SEVERE, "GetMetrics request received for unknown instanceId " + id);
          response.status.status = 2;
          return;
        } else {
          metricsInstance.get(id).GetMetrics(request, startTime, endTime, response);
          if (response.status.status != 1) {
            return;
          }
        }
      }
    }
    response.status.status = 1;
  }
}
