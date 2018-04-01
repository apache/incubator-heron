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
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import com.twitter.heron.proto.system.Common.StatusCode;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricInterval;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.IndividualMetric;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.IndividualMetric.IntervalValue;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.TaskMetric;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricsCacheLocation;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;

import static com.twitter.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;

public class MetricsCacheMetricsProvider implements MetricsProvider {
  private static final String PATH_STATS = "stats";
  private static final Logger LOG = Logger.getLogger(MetricsCacheMetricsProvider.class.getName());

  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  private Clock clock = new Clock();
  private String metricsCacheLocation;

  @Inject
  public MetricsCacheMetricsProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
                                     @Named(CONF_TOPOLOGY_NAME) String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;

    LOG.info("Metrics will be provided by MetricsCache at " + getCacheLocation());
  }

  @Override
  public Map<String, ComponentMetrics> getComponentMetrics(String metric,
                                                           Instant startTime,
                                                           Duration duration,
                                                           String... components) {
    Map<String, ComponentMetrics> result = new HashMap<>();
    for (String component : components) {
      TopologyMaster.MetricResponse response =
          getMetricsFromMetricsCache(metric, component, startTime, duration);

      Map<String, InstanceMetrics> metrics = parse(response, component, metric, startTime);
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
      TopologyMaster.MetricResponse response, String component, String metric, Instant startTime) {
    Map<String, InstanceMetrics> metricsData = new HashMap<>();

    if (response == null || !response.getStatus().getStatus().equals(StatusCode.OK)) {
      LOG.info(String.format(
          "Query failure from MetricsCache for %s:%s ", component, metric));
      return metricsData;
    }

    if (response.getMetricCount() == 0) {
      LOG.info(String.format(
          "Did not get any metrics from MetricsCache for %s:%s ", component, metric));
      return metricsData;
    }

    // convert heron.protobuf.taskMetrics to dhalion.InstanceMetrics
    for (TaskMetric tm : response.getMetricList()) {
      String instanceId = tm.getInstanceId();
      InstanceMetrics instanceMetrics = new InstanceMetrics(instanceId);

      for (IndividualMetric im : tm.getMetricList()) {
        String metricName = im.getName();
        Map<Instant, Double> values = new HashMap<>();

        // case 1
        for (IntervalValue iv : im.getIntervalValuesList()) {
          MetricInterval mi = iv.getInterval();
          String value = iv.getValue();
          values.put(Instant.ofEpochSecond(mi.getStart()), Double.parseDouble(value));
        }
        // case 2
        if (im.hasValue()) {
          values.put(startTime, Double.parseDouble(im.getValue()));
        }

        if (!values.isEmpty()) {
          instanceMetrics.addMetric(metricName, values);
        }
      }

      metricsData.put(instanceId, instanceMetrics);
    }

    return metricsData;
  }

  @VisibleForTesting
  TopologyMaster.MetricResponse getMetricsFromMetricsCache(
      String metric, String component, Instant start, Duration duration) {
    LOG.log(Level.FINE, "MetricsCache Query request metric name : {0}", metric);
    TopologyMaster.MetricRequest request = TopologyMaster.MetricRequest.newBuilder()
        .setComponentName(component)
        .setExplicitInterval(
            MetricInterval.newBuilder()
                .setStart(start.getEpochSecond())
                .setEnd(start.plus(duration).getEpochSecond())
                .build())
        .addMetric(metric)
        .build();
    LOG.log(Level.FINE, "MetricsCache Query request: \n{0}", request);

    HttpURLConnection connection = NetworkUtils.getHttpConnection(getCacheLocation());
    try {
      boolean result = NetworkUtils.sendHttpPostRequest(connection, "X", request.toByteArray());
      if (!result) {
        LOG.warning("Failed to get response from metrics cache. Resetting connection...");
        resetCacheLocation();
        return null;
      }

      byte[] responseData = NetworkUtils.readHttpResponse(connection);

      try {
        TopologyMaster.MetricResponse response =
            TopologyMaster.MetricResponse.parseFrom(responseData);
        LOG.log(Level.FINE, "MetricsCache Query response: \n{0}", response);
        return response;
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "protobuf cannot parse the reply from MetricsCache ", e);
        return null;
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @VisibleForTesting
  void setClock(Clock clock) {
    this.clock = clock;
  }

  /* returns last known location of metrics cache
   */
  private synchronized String getCacheLocation() {
    if (metricsCacheLocation != null) {
      return metricsCacheLocation;
    }

    MetricsCacheLocation cacheLocation = stateManagerAdaptor.getMetricsCacheLocation(topologyName);
    metricsCacheLocation = String.format("http://%s:%s/%s", cacheLocation.getHost(),
        cacheLocation.getStatsPort(), PATH_STATS);
    return metricsCacheLocation;
  }

  private synchronized void resetCacheLocation() {
    metricsCacheLocation = null;
  }

  static class Clock {
    long currentTime() {
      return System.currentTimeMillis();
    }
  }
}
