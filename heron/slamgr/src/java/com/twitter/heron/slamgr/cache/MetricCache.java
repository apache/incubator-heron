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


package com.twitter.heron.slamgr.cache;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricDatum;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricRequest;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse;
import com.twitter.heron.proto.tmaster.TopologyMaster.PublishMetrics;
import com.twitter.heron.slamgr.SLAMetrics;


public class MetricCache {
  private static final Logger LOG = Logger.getLogger(MetricCache.class.getName());

  // map of component name to its metrics
  Map<String, ComponentMetrics> metrics_component_;
  int max_interval_;
  int nintervals_;
  int interval_;
  String metrics_sinks_yaml_;
  SLAMetrics tmetrics_info_;
  int start_time_;

  MetricCache(int _max_interval, String metrics_sinks_yaml) {
    max_interval_ = _max_interval;
    metrics_sinks_yaml_ = metrics_sinks_yaml;
    tmetrics_info_ = new SLAMetrics(metrics_sinks_yaml);
    start_time_ = (int) new Date().getTime();

    interval_ = 60; // from heron_internals.yaml
    nintervals_ = max_interval_ / interval_;

    metrics_component_ = new HashMap<>();
  }

  public SLAMetrics getSLAMetrics() {
    return tmetrics_info_;
  }

  public void AddMetric(PublishMetrics _metrics) {
    for (int i = 0; i < _metrics.getMetricsCount(); ++i) {
      String component_name = _metrics.getMetrics(i).getComponentName();
      AddMetricsForComponent(component_name, _metrics.getMetrics(i));
    }
  }

  void AddMetricsForComponent(String component_name, MetricDatum metrics_data) {
    ComponentMetrics component_metrics = GetOrCreateComponentMetrics(component_name);
    String name = metrics_data.getName();
    SLAMetrics.MetricAggregationType type = tmetrics_info_.GetAggregationType(name);
    component_metrics.AddMetricForInstance(metrics_data.getInstanceId(), name, type,
        metrics_data.getValue());
  }

  ComponentMetrics GetOrCreateComponentMetrics(String component_name) {
    if (!metrics_component_.containsKey(component_name)) {
      metrics_component_.put(component_name, new ComponentMetrics(component_name, nintervals_, interval_));
    }
    return metrics_component_.get(component_name);
  }

  // Returns a new response to fetch metrics. The request gets propagated to Component's and
  // Instance's get metrics. Doesn't own Response.
  MetricResponse GetMetrics(MetricRequest _request) {
    MetricResponse.Builder response_builder = MetricResponse.newBuilder();

    if (!metrics_component_.containsKey(_request.getComponentName())) {
      LOG.log(Level.WARNING,
          "Metrics for component `" + _request.getComponentName() + "` are not available");
      response_builder.setStatus(response_builder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("Metrics not available for component `" + _request.getComponentName() + "`")
          .build());
    } else if (!_request.hasInterval() && !_request.hasExplicitInterval()) {
      LOG.log(Level.SEVERE,
          "GetMetrics request does not have either interval" + " nor explicit interval");
      response_builder.setStatus(response_builder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("No interval or explicit interval set")
          .build());
    } else {
      long start_time, end_time;
      if (_request.hasInterval()) {
        end_time = new Date().getTime();
        if (_request.getInterval() <= 0) {
          start_time = 0;
        } else {
          start_time = end_time - _request.getInterval();
        }
      } else {
        start_time = _request.getExplicitInterval().getStart();
        end_time = _request.getExplicitInterval().getEnd();
      }
      metrics_component_.get(_request.getComponentName())
          .GetMetrics(_request, start_time, end_time, response_builder);
      response_builder.setInterval(end_time - start_time);
    }

    return response_builder.build();
  }

  public void Purge() {
    for (ComponentMetrics cm : metrics_component_.values()) {
      cm.Purge();
    }
  }


}