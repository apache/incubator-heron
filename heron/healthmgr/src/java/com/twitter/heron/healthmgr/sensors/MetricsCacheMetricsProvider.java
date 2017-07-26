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

import java.net.HttpURLConnection;
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
import com.twitter.heron.proto.system.Common.StatusCode;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricInterval;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.TaskMetric;
import com.twitter.heron.spi.utils.NetworkUtils;

import net.minidev.json.JSONArray;

import static com.twitter.heron.healthmgr.HealthManager.CONF_METRICS_SOURCE_URL;
import static com.twitter.heron.metricscachemgr.MetricsCacheManagerHttpServer.PATH_STATS;

public class MetricsCacheMetricsProvider implements MetricsProvider {
  private static final Logger LOG = Logger.getLogger(MetricsCacheMetricsProvider.class.getName());
  private HttpURLConnection con;

  private Clock clock = new Clock();

  @Inject
  public MetricsCacheMetricsProvider(@Named(CONF_METRICS_SOURCE_URL) String metricsCacheURL) {
    LOG.info("Metrics will be provided by MetricsCache at :" + metricsCacheURL);

    String url = metricsCacheURL + PATH_STATS;
    con = NetworkUtils.getHttpConnection(url);
  }

  @Override
  public Map<String, ComponentMetrics> getComponentMetrics(String metric,
                                                           Instant startTime,
                                                           Duration duration,
                                                           String... components) {
    Map<String, ComponentMetrics> result = new HashMap<>();
    for (String component : components) {
      String response = getMetricsFromMetricsCache(metric, component, startTime, duration);
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

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Map<String, InstanceMetrics> parse(
      TopologyMaster.MetricResponse response, String component, String metric) {
    Map<String, InstanceMetrics> metricsData = new HashMap<>();

    if (response == null || !response.getStatus().getStatus().equals(StatusCode.OK)) {
      return metricsData;
    }

    if (response.getMetricCount() == 0) {
      LOG.info(String.format("Did not get any metrics from tracker for %s:%s ", component, metric));
      return metricsData;
    }

    Map<String, Object> metricsMap = (Map<String, Object>) jsonArray.get(0);
    if (metricsMap == null || metricsMap.isEmpty()) {
      LOG.info(String.format("Did not get any metrics from tracker for %s:%s ", component, metric));
      return metricsData;
    }

    for (TaskMetric tm :response.getMetricList()) {
      // TODO(huijun): convert heron.protobuf.taskMetrics to dhalion.InstanceMetrics
    }
//    for (String instanceName : metricsMap.keySet()) {
//      Map<String, String> tmpValues = (Map<String, String>) metricsMap.get(instanceName);
//      Map<Instant, Double> values = new HashMap<>();
//      for (String timeStamp : tmpValues.keySet()) {
//        values.put(Instant.ofEpochSecond(Long.parseLong(timeStamp)),
//            Double.parseDouble(tmpValues.get(timeStamp)));
//      }
//      InstanceMetrics instanceMetrics = new InstanceMetrics(instanceName);
//      instanceMetrics.addMetric(metric, values);
//      metricsData.put(instanceName, instanceMetrics);
//    }

    return metricsData;
  }

  @VisibleForTesting
  TopologyMaster.MetricResponse getMetricsFromMetricsCache(String metric, String component, Instant start, Duration duration) {
    TopologyMaster.MetricRequest request = TopologyMaster.MetricRequest.newBuilder()
        .setComponentName(component)
        .setExplicitInterval(
            MetricInterval.newBuilder()
            .setStart(start.getEpochSecond())
            .setEnd(start.plus(duration).getEpochSecond())
            .build())
        .addMetric(metric)
        .build();
    LOG.info("MetricsCache Query request: " + request.toString());

    NetworkUtils.sendHttpPostRequest(con, "X", request.toByteArray());
    byte[] responseData = NetworkUtils.readHttpResponse(con);
    
    TopologyMaster.MetricResponse response = 
      TopologyMaster.MetricResponse.parseFrom(responseData);
    LOG.info("MetricsCache Query response: " + response.toString());
    return response;
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
