//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.metricscachemgr.metriccache;


import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.metricscachemgr.SLAMetrics;

// Component level metrics. A component metrics is a map storing metrics for each of its
// instance as 'InstanceMetrics'.
public class ComponentMetrics {
  private static final Logger LOG = Logger.getLogger(Metric.class.getName());

  private String component_name_;
  private int nbuckets_;
  private int bucket_interval_;
  // map between instance id and its set of metrics
  private Map<String, InstanceMetrics> metrics_instance_;

  // ctor. '_component_name' is the user supplied name given to the spout/bolt. '_nbuckets' is
  // number of buckets stored for this component.
  public ComponentMetrics(String component_name, int nbuckets, int bucket_interval) {
    component_name_ = component_name;
    nbuckets_ = nbuckets;
    bucket_interval_ = bucket_interval;

    metrics_instance_ = new HashMap<>();
  }

  // Remove old metrics and exception associated with this spout/bolt component.
  public void Purge() {
    for (InstanceMetrics im : metrics_instance_.values()) {
      im.Purge();
    }
  }

  // Add metrics for an Instance 'instance_id' of this spout/bolt component.
  public void AddMetricForInstance(String instance_id, String name,
                                   SLAMetrics.MetricAggregationType type, String value) {
    InstanceMetrics instance_metrics = GetOrCreateInstanceMetrics(instance_id);
    instance_metrics.AddMetricWithName(name, type, value);
  }

  // Request aggregated metrics for this component for the '_nbucket' interval.
  // Doesn't own '_response' object.
  public void GetMetrics(TopologyMaster.MetricRequest _request, long start_time, long end_time,
                         TopologyMaster.MetricResponse.Builder _response_builder) {
    if (_request.getInstanceIdCount() == 0) {
      // This means that all instances need to be returned
      for (InstanceMetrics im : metrics_instance_.values()) {
        im.GetMetrics(_request, start_time, end_time, _response_builder);
        if (_response_builder.getStatus().getStatus() != Common.StatusCode.OK) {
          return;
        }
      }
    } else {
      for (int i = 0; i < _request.getInstanceIdCount(); ++i) {
        String id = _request.getInstanceId(i);
        if (!metrics_instance_.containsKey(id)) {
          LOG.log(Level.SEVERE, "GetMetrics request received for unknown instance_id " + id);
          _response_builder.setStatus(_response_builder.getStatusBuilder()
              .setStatus(Common.StatusCode.NOTOK).build());
          return;
        } else {
          metrics_instance_.get(id).GetMetrics(_request, start_time, end_time, _response_builder);
          if (_response_builder.getStatus().getStatus() != Common.StatusCode.OK) {
            return;
          }
        }
      }
    }
    _response_builder.setStatus(_response_builder.getStatusBuilder()
        .setStatus(Common.StatusCode.OK).build());
  }


  // Create or return existing mutable InstanceMetrics associated with 'instance_id'. This
  // method doesn't verify if the instance_id is valid fof the component.
  // Doesn't transfer ownership of returned InstanceMetrics.
  private InstanceMetrics GetOrCreateInstanceMetrics(String instance_id) {
    if (!metrics_instance_.containsKey(instance_id)) {
      metrics_instance_.put(instance_id, new InstanceMetrics(instance_id, nbuckets_, bucket_interval_));
    }
    return metrics_instance_.get(instance_id);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("component name: ").append(component_name_).append("; nbuckets: ").append(nbuckets_).append("; bucket_interval: ").append(bucket_interval_).append("; metrics_instance:");
    for (String k : metrics_instance_.keySet()) {
      sb.append("\n ").append(k).append(" ~> ").append(metrics_instance_.get(k).toString());
    }
    return sb.toString();
  }
}