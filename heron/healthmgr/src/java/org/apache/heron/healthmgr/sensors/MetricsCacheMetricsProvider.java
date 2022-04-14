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

import java.net.HttpURLConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.core.Measurement;

import org.apache.heron.proto.system.Common.StatusCode;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.proto.tmanager.TopologyManager.MetricInterval;
import org.apache.heron.proto.tmanager.TopologyManager.MetricResponse.IndividualMetric;
import org.apache.heron.proto.tmanager.TopologyManager.MetricResponse.IndividualMetric.IntervalValue;
import org.apache.heron.proto.tmanager.TopologyManager.MetricResponse.TaskMetric;
import org.apache.heron.proto.tmanager.TopologyManager.MetricsCacheLocation;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.NetworkUtils;

import static org.apache.heron.common.basics.TypeUtils.getDouble;
import static org.apache.heron.healthmgr.HealthPolicyConfig.CONF_TOPOLOGY_NAME;

public class MetricsCacheMetricsProvider implements MetricsProvider {
  private static final String PATH_STATS = "stats";
  private static final Logger LOG = Logger.getLogger(MetricsCacheMetricsProvider.class.getName());

  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  private String metricsCacheLocation;

  @Inject
  public MetricsCacheMetricsProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
                                     @Named(CONF_TOPOLOGY_NAME) String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;

    LOG.info("Metrics will be provided by MetricsCache at " + getCacheLocation());
  }

  @Override
  public Collection<Measurement> getMeasurements(Instant startTime,
                                                 Duration duration,
                                                 Collection<String> metricNames,
                                                 Collection<String> components) {
    Collection<Measurement> result = new ArrayList<>();
    for (String metric : metricNames) {
      for (String component : components) {
        TopologyManager.MetricResponse response =
            getMetricsFromMetricsCache(metric, component, startTime, duration);
        Collection<Measurement> measurements = parse(response, component, metric, startTime);
        LOG.fine(String.format("%d measurements received for %s/%s",
            measurements.size(), component, metric));
        result.addAll(measurements);
      }
    }
    return result;
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Collection<Measurement> parse(
      TopologyManager.MetricResponse response, String component, String metric, Instant startTime) {
    Collection<Measurement> metricsData = new ArrayList();

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
      for (IndividualMetric im : tm.getMetricList()) {
        String metricName = im.getName();

        // case 1
        for (IntervalValue iv : im.getIntervalValuesList()) {
          MetricInterval mi = iv.getInterval();
          String value = iv.getValue();
          Measurement measurement = new Measurement(
              component,
              instanceId,
              metricName,
              Instant.ofEpochSecond(mi.getStart()),
              getDouble(value));
          metricsData.add(measurement);
        }
        // case 2
        if (im.hasValue()) {
          Measurement measurement = new Measurement(
              component,
              instanceId,
              metricName,
              startTime,
              getDouble(im.getValue()));
          metricsData.add(measurement);
        }
      }
    }

    return metricsData;
  }

  @VisibleForTesting
  TopologyManager.MetricResponse getMetricsFromMetricsCache(
      String metric, String component, Instant start, Duration duration) {
    LOG.log(Level.FINE, "MetricsCache Query request metric name : {0}", metric);
    TopologyManager.MetricRequest request = TopologyManager.MetricRequest.newBuilder()
        .setComponentName(component)
        .setExplicitInterval(
            MetricInterval.newBuilder()
                .setStart(start.minus(duration).getEpochSecond())
                .setEnd(start.getEpochSecond())
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
        TopologyManager.MetricResponse response =
            TopologyManager.MetricResponse.parseFrom(responseData);
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
}
