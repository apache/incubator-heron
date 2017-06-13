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


package com.twitter.heron.healthmgr.sensors;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.annotations.VisibleForTesting;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import net.minidev.json.JSONArray;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;

public class TrackerMetricsProvider implements MetricsProvider {
  private static final Logger LOG = Logger.getLogger(TrackerMetricsProvider.class.getName());
  private final WebTarget baseTarget;

  @Inject
  public TrackerMetricsProvider(@Named(HealthMgrConstants.CONF_TRACKER_URL) String trackerURL,
                                @Named(HealthMgrConstants.CONF_TOPOLOGY_NAME) String topologyName,
                                @Named(HealthMgrConstants.CONF_CLUSTER) String cluster,
                                @Named(HealthMgrConstants.CONF_ENVIRON) String environ) {
    LOG.info("Metrics will be provided by tracker at :" + trackerURL);

    Client client = ClientBuilder.newClient();
    this.baseTarget = client.target(trackerURL)
        .path("topologies/metrics")
        .queryParam("cluster", cluster)
        .queryParam("environ", environ)
        .queryParam("topology", topologyName);
  }

  @Override
  public Map<String, ComponentMetrics> getComponentMetrics(String metric,
                                                           int durationSec,
                                                           String... components) {
    Map<String, ComponentMetrics> result = new HashMap<>();
    for (String component : components) {
      String response = getMetricsFromTracker(metric, component, durationSec);
      Map<String, InstanceMetrics> metrics = parse(response, component, metric);
      ComponentMetrics componentMetric = new ComponentMetrics(component, metrics);
      result.put(component, componentMetric);
    }

    return result;
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Map<String, InstanceMetrics> parse(String response, String component, String metric) {
    Map<String, InstanceMetrics> metricsData = new HashMap<>();

    if (response == null || response.isEmpty()) {
      return metricsData;
    }

    DocumentContext result = JsonPath.parse(response);
    JSONArray jsonArray = result.read("$.." + metric);
    if (jsonArray.size() != 1) {
      LOG.info(String.format("Did not get any metrics from tracker for %s:%s ", component, metric));
      return metricsData;
    }

    Map<String, Object> metricsMap = (Map<String, Object>) jsonArray.get(0);
    if (metricsMap == null || metricsMap.isEmpty()) {
      LOG.info(String.format("Did not get any metrics from tracker for %s:%s ", component, metric));
      return metricsData;
    }

    for (String instanceName : metricsMap.keySet()) {
      Object tmpValue = metricsMap.get(instanceName);
      if (tmpValue instanceof String) {
        // response for a single metric request
        double value = Double.parseDouble((String) tmpValue);
        metricsData.put(instanceName, new InstanceMetrics(instanceName, metric, value));
      } else if (tmpValue instanceof Map) {
        // response for the timeline request
        Map<String, String> tmpValues = (Map<String, String>) tmpValue;
        Map<Long, Double> values = new HashMap<>();
        for (String timeStamp : tmpValues.keySet()) {
          values.put(Long.parseLong(timeStamp), Double.parseDouble(tmpValues.get(timeStamp)));
        }
        InstanceMetrics instanceMetrics = new InstanceMetrics(instanceName);
        instanceMetrics.addMetric(metric, values);
        metricsData.put(instanceName, instanceMetrics);
      }
    }

    return metricsData;
  }

  @Override
  public void close() {

  }

  @VisibleForTesting
  String getMetricsFromTracker(String metric, String component, int durationSec) {
    WebTarget target = baseTarget
        .queryParam("metricname", metric)
        .queryParam("component", component)
        .queryParam("interval", durationSec);
//    if (instance != null) {
//      target.queryParam("instance", instance.getName());
//    }

    LOG.fine("Tracker Query URI: " + target.getUri());

    Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    return r.readEntity(String.class);
  }

  @VisibleForTesting
  String getMetricsFromTracker(String metric, String component, int startTimeSec, int durationSec) {
    WebTarget target = baseTarget
        .queryParam("metricname", metric)
        .queryParam("component", component)
        .queryParam("interval", durationSec);
//    if (instance != null) {
//      target.queryParam("instance", instance.getName());
//    }

    LOG.fine("Tracker Query URI: " + target.getUri());

    Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    return r.readEntity(String.class);
  }
}
