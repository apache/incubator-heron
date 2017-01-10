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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;

/**
 * define the query request and response format
 */
public final class MetricsCacheQueryUtils {
  private MetricsCacheQueryUtils() {
  }

  public static MetricCacheRequest Convert(TopologyMaster.MetricRequest request) {
    MetricCacheRequest request1 = new MetricCacheRequest();

    request1.componentName = request.getComponentName();
    if (request.getInstanceIdCount() > 0) {
      request1.instanceId.addAll(request.getInstanceIdList());
    }
    if (request.getMetricCount() > 0) {
      request1.metric.addAll(request.getMetricList());
    }
    if (request.hasInterval()) {
      request1.interval.end = Instant.now().getEpochSecond();
      if (request.getInterval() <= 0) {
        request1.interval.start = 0;
      } else {
        request1.interval.start = request1.interval.end - request.getInterval();
      }
    } else {
      request1.interval.start = request.getExplicitInterval().getStart();
      request1.interval.end = request.getExplicitInterval().getEnd();
    }
    if (request.hasMinutely()) {
      request1.minutely = request.getMinutely();
    }

    return request1;
  }

  public static TopologyMaster.MetricResponse Convert(MetricCacheResponse response) {
    TopologyMaster.MetricResponse.Builder ret = TopologyMaster.MetricResponse.newBuilder();

    switch (response.status.status) {
      case 1:
        ret.setStatus(Common.Status.newBuilder()
            .setStatus(Common.StatusCode.OK)
            .setMessage(response.status.message));
        break;
      case 2:
        ret.setStatus(Common.Status.newBuilder()
            .setStatus(Common.StatusCode.NOTOK)
            .setMessage(response.status.message));
        break;
      default:
        ret.setStatus(Common.Status.newBuilder()
            .setStatus(Common.StatusCode.NOTOK)
            .setMessage(response.status.message));
        break;
    }

    if (response.interval > 0) {
      ret.setInterval(response.interval);
    }

    for (TaskMetric tm : response.metric) {
      TopologyMaster.MetricResponse.TaskMetric tm1 = Convert(tm);
      ret.addMetric(tm1);
    }

    return ret.build();
  }

  private static TopologyMaster.MetricResponse.TaskMetric Convert(TaskMetric tm) {
    TopologyMaster.MetricResponse.TaskMetric.Builder ret =
        TopologyMaster.MetricResponse.TaskMetric.newBuilder();

    ret.setInstanceId(tm.instanceId);

    for (IndividualMetric im : tm.metric) {
      TopologyMaster.MetricResponse.IndividualMetric im1 = Convert(im);
      ret.addMetric(im1);
    }

    return ret.build();
  }

  private static TopologyMaster.MetricResponse.IndividualMetric Convert(IndividualMetric im) {
    TopologyMaster.MetricResponse.IndividualMetric.Builder ret =
        TopologyMaster.MetricResponse.IndividualMetric.newBuilder();

    ret.setName(im.name);

    if (im.value != null) {
      ret.setValue(im.value);
    }

    for (IntervalValue iv : im.intervalValues) {
      TopologyMaster.MetricResponse.IndividualMetric.IntervalValue iv1 = Convert(iv);
      ret.addIntervalValues(iv1);
    }

    return ret.build();
  }

  private static TopologyMaster.MetricResponse.IndividualMetric.IntervalValue Convert(
      IntervalValue iv) {
    TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.Builder ret =
        TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.newBuilder();

    ret.setValue(iv.value);
    ret.setInterval(TopologyMaster.MetricInterval.newBuilder()
        .setStart(iv.interval.start)
        .setEnd(iv.interval.end)
        .build());

    return ret.build();
  }

  public static class MetricInterval {
    public long start = 0;
    public long end = Long.MAX_VALUE;
  }

  public static class MetricCacheRequest {
    public String componentName;
    // what timeframe data in seconds
    // default value means everything
    // What is the time interval that you want the metrics for
    // Clients can specify one in many ways.
    // 1. start<0. Essentially abs(start) seconds worth of metrics before the current time
    // 2. explicit_interval. An explicit interval
    public MetricInterval interval = new MetricInterval();
    // Do you want metrics broken down on a per minute basis?
    public boolean minutely = false;
    // The instance ids to get the stats from
    // If nothing is specified, we will get from
    // all the instances of the component name
    public List<String> instanceId = new ArrayList<>();
    // What set of metrics you are interested in
    // Example is __emit-count/default
    public List<String> metric = new ArrayList<>();
  }

  public static class TaskMetric {
    public String instanceId;
    public List<IndividualMetric> metric = new ArrayList<>();
  }

  public static class IndividualMetric {
    public String name;
    public String value = null;
    public List<IntervalValue> intervalValues = new ArrayList<>();
  }

  public static class IntervalValue {
    public String value;
    public MetricInterval interval = new MetricInterval();
  }

  public static class Status {
    public int status = 2;
    public String message = null;
  }

  public static class MetricCacheResponse {
    // The order is the same as the request
    public List<TaskMetric> metric = new ArrayList<>();
    // The interval we got the data for.
    // This may not be present in case status is NOTOK
    public long interval = -1;
    public Status status = new Status();
  }
}
