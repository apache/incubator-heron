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

package org.apache.heron.healthmgr.sensors;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
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
import com.microsoft.dhalion.core.Measurement;

import net.minidev.json.JSONArray;

import static org.apache.heron.common.basics.TypeUtils.getDouble;
import static org.apache.heron.healthmgr.HealthPolicyConfig.CONF_METRICS_SOURCE_URL;
import static org.apache.heron.healthmgr.HealthPolicyConfig.CONF_TOPOLOGY_NAME;

public class TrackerMetricsProvider implements MetricsProvider {
  public static final String CONF_CLUSTER = "CLUSTER";
  public static final String CONF_ENVIRON = "ENVIRON";

  private static final Logger LOG = Logger.getLogger(TrackerMetricsProvider.class.getName());
  private final WebTarget baseTarget;

  @Inject
  public TrackerMetricsProvider(@Named(CONF_METRICS_SOURCE_URL) String trackerURL,
                                @Named(CONF_TOPOLOGY_NAME) String topologyName,
                                @Named(CONF_CLUSTER) String cluster,
                                @Named(CONF_ENVIRON) String environ) {
    LOG.info("Metrics will be provided by tracker at :" + trackerURL);

    Client client = ClientBuilder.newClient();

    this.baseTarget = client.target(trackerURL)
        .path("topologies/metrics/timeline")
        .queryParam("cluster", cluster)
        .queryParam("environ", environ)
        .queryParam("topology", topologyName);
  }

  @Override
  public Collection<Measurement> getMeasurements(Instant startTime,
                                                 Duration duration,
                                                 Collection<String> metricNames,
                                                 Collection<String> components) {
    Collection<Measurement> result = new ArrayList<>();
    for (String metric : metricNames) {
      for (String component : components) {
        String response = getMetricsFromTracker(metric, component, startTime, duration);
        Collection<Measurement> measurements = parse(response, component, metric);
        LOG.fine(String.format("%d measurements received for %s/%s",
            measurements.size(), component, metric));
        result.addAll(measurements);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private Collection<Measurement> parse(String response, String component, String metric) {
    Collection<Measurement> metricsData = new ArrayList();

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
      Map<String, Object> tmpValues = (Map<String, Object>) metricsMap.get(instanceName);
      for (String timeStamp : tmpValues.keySet()) {
        Measurement measurement = new Measurement(
            component,
            instanceName,
            metric,
            Instant.ofEpochSecond(Long.parseLong(timeStamp)),
            getDouble(tmpValues.get(timeStamp)));
        metricsData.add(measurement);
      }
    }

    return metricsData;
  }

  @VisibleForTesting
  String getMetricsFromTracker(String metric, String component, Instant start, Duration duration) {
    WebTarget target = baseTarget
        .queryParam("metricname", metric)
        .queryParam("component", component)
        .queryParam("starttime", start.getEpochSecond() - duration.getSeconds())
        .queryParam("endtime", start.getEpochSecond());

    LOG.log(Level.FINE, "Tracker Query URI: {0}", target.getUri());

    Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    return r.readEntity(String.class);
  }
}
