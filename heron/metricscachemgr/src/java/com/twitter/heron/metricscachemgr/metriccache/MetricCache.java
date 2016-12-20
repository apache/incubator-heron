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


package com.twitter.heron.metricscachemgr.metriccache;

import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.metricscachemgr.CollectorMetrics;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricDatum;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricRequest;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse;
import com.twitter.heron.proto.tmaster.TopologyMaster.PublishMetrics;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter.MetricAggregationType;

public class MetricCache {
  private static final Logger LOG = Logger.getLogger(MetricCache.class.getName());

  // map of component name to its metrics
  private Map<String, ComponentMetrics> metricsComponent;
  private int maxInterval;
  private int nintervals;
  private int interval;
  private String metricsSinksYaml;
  private CollectorMetrics tmetricsInfo;
//  private int startTime;

  MetricCache(int maxInterval, String metricsSinksYaml) throws FileNotFoundException {
    this.maxInterval = maxInterval;
    this.metricsSinksYaml = metricsSinksYaml;
    tmetricsInfo = new CollectorMetrics(metricsSinksYaml);
//    startTime = (int) Instant.now().getEpochSecond();

    interval = 5; // 60 from heron_internals.yaml; 5 for debug
    nintervals = maxInterval / interval;

    metricsComponent = new HashMap<>();
  }

  public CollectorMetrics getSLAMetrics() {
    return tmetricsInfo;
  }

  public void AddMetric(PublishMetrics metrics) {
    for (int i = 0; i < metrics.getMetricsCount(); ++i) {
      String componentName = metrics.getMetrics(i).getComponentName();
      AddMetricsForComponent(componentName, metrics.getMetrics(i));
    }
  }

  void AddMetricsForComponent(String componentName, MetricDatum metricsData) {
    ComponentMetrics componentmetrics = GetOrCreateComponentMetrics(componentName);
    String name = metricsData.getName();
    MetricAggregationType type = tmetricsInfo.getMetricsFilter().getAggregationType(name);
    componentmetrics.AddMetricForInstance(metricsData.getInstanceId(), name, type,
        metricsData.getValue());
  }

  ComponentMetrics GetOrCreateComponentMetrics(String componentName) {
    if (!metricsComponent.containsKey(componentName)) {
      metricsComponent.put(componentName,
          new ComponentMetrics(componentName, nintervals, interval));
    }
    return metricsComponent.get(componentName);
  }

  // Returns a new response to fetch metrics. The request gets propagated to Component's and
  // Instance's get metrics. Doesn't own Response.
  MetricResponse GetMetrics(MetricRequest request) {
    MetricResponse.Builder responseBuilder = MetricResponse.newBuilder();

    if (!metricsComponent.containsKey(request.getComponentName())) {
      LOG.log(Level.WARNING,
          "Metrics for component `" + request.getComponentName() + "` are not available");
      responseBuilder.setStatus(responseBuilder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("Metrics not available for component `" + request.getComponentName() + "`")
          .build());
    } else if (!request.hasInterval() && !request.hasExplicitInterval()) {
      LOG.log(Level.SEVERE,
          "GetMetrics request does not have either interval" + " nor explicit interval");
      responseBuilder.setStatus(responseBuilder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("No interval or explicit interval set")
          .build());
    } else {
      long startTime;
      long endTime;
      if (request.hasInterval()) {
        endTime = Instant.now().getEpochSecond();
        if (request.getInterval() <= 0) {
          startTime = 0;
        } else {
          startTime = endTime - request.getInterval();
        }
      } else {
        startTime = request.getExplicitInterval().getStart();
        endTime = request.getExplicitInterval().getEnd();
      }
      System.err.println("startTime: " + startTime + "; endTime: " + endTime);
      metricsComponent.get(request.getComponentName())
          .GetMetrics(request, startTime, endTime, responseBuilder);
      responseBuilder.setInterval(endTime - startTime);
    }

    return responseBuilder.build();
  }

  public void Purge() {
    for (ComponentMetrics cm : metricsComponent.values()) {
      cm.Purge();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String k : metricsComponent.keySet()) {
      sb.append("\n").append(k).append(" #> ").append(metricsComponent.get(k).toString());
    }
    return sb.toString();
  }
}
