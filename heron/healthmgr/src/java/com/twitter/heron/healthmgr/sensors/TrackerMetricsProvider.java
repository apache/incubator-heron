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

import java.time.Duration;
import java.time.Instant;
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

import static com.twitter.heron.healthmgr.HealthManager.CONF_METRICS_SOURCE_URL;
import static com.twitter.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;

public class TrackerMetricsProvider implements MetricsProvider {
  public static final String CONF_CLUSTER = "CLUSTER";
  public static final String CONF_ENVIRON = "ENVIRON";

  private static final Logger LOG = Logger.getLogger(TrackerMetricsProvider.class.getName());
  private final WebTarget baseTarget;

  private Clock clock = new Clock();

  @Inject
  public TrackerMetricsProvider(@Named(CONF_METRICS_SOURCE_URL) String trackerURL,
                                @Named(CONF_TOPOLOGY_NAME) String topologyName,
                                @Named(CONF_CLUSTER) String cluster,
                                @Named(CONF_ENVIRON) String environ) {
    LOG.info("Metrics will be provided by tracker at :" + trackerURL);

    Client client = ClientBuilder.newClient();

    this.baseTarget = client.target(trackerURL)
        .path("topologies/metricstimeline")
        .queryParam("cluster", cluster)
        .queryParam("environ", environ)
        .queryParam("topology", topologyName);
  }

  @Override
  public Map<String, ComponentMetrics> getComponentMetrics(String metric,
                                                           Instant startTime,
                                                           Duration duration,
                                                           String... components) {
    Map<String, ComponentMetrics> result = new HashMap<>();
    for (String component : components) {
      String response = getMetricsFromTracker(metric, component, startTime, duration);
      Map<String, InstanceMetrics> metrics = parse(response, component, metric);
      ComponentMetrics componentMetric = new ComponentMetrics(component, metrics);
      result.put(component, componentMetric);
    }
    return result;
  }

  @Override
  public Map<String, ComponentMetrics> getComponentMetrics(String metric,
                                                           Duration duration,
                                                           String... components) {
    Instant start = Instant.ofEpochMilli(clock.currentTime() - duration.toMillis());
    return getComponentMetrics(metric, start, duration, components);
  }

  @SuppressWarnings("unchecked")
  private Map<String, InstanceMetrics> parse(String response, String component, String metric) {
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
      Map<String, String> tmpValues = (Map<String, String>) metricsMap.get(instanceName);
      Map<Instant, Double> values = new HashMap<>();
      for (String timeStamp : tmpValues.keySet()) {
        values.put(Instant.ofEpochSecond(Long.parseLong(timeStamp)),
            Double.parseDouble(tmpValues.get(timeStamp)));
      }
      InstanceMetrics instanceMetrics = new InstanceMetrics(instanceName);
      instanceMetrics.addMetric(metric, values);
      metricsData.put(instanceName, instanceMetrics);
    }

    return metricsData;
  }

  @VisibleForTesting
  String getMetricsFromTracker(String metric, String component, Instant start, Duration duration) {
    WebTarget target = baseTarget
        .queryParam("metricname", metric)
        .queryParam("component", component)
        .queryParam("starttime", start.getEpochSecond())
        .queryParam("endtime", start.getEpochSecond() + duration.getSeconds());

    LOG.info("Tracker Query URI: " + target.getUri());

    Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    return r.readEntity(String.class);
  }

  @VisibleForTesting
  void setClock(Clock clock) {
    this.clock = clock;
  }

  static class Clock {
    long currentTime() {
      return System.currentTimeMillis();
    }
  }
}
