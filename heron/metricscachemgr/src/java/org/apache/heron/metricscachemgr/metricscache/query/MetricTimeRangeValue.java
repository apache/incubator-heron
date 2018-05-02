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

package org.apache.heron.metricscachemgr.metricscache.query;

import org.apache.heron.metricscachemgr.metricscache.store.MetricDatapoint;

/**
 * immutable data bag for time range value
 * time window: startTime ~ endTime, in milli-seconds
 * metric value string: value
 */
public final class MetricTimeRangeValue {
  private final long startTime;
  private final long endTime;
  private final String value;

  public MetricTimeRangeValue(long startTime, long endTime, String value) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.value = value;
  }

  public MetricTimeRangeValue(MetricDatapoint metricDatapoint) {
    this.startTime = metricDatapoint.getTimestamp();
    this.endTime = metricDatapoint.getTimestamp();
    this.value = metricDatapoint.getValue();
  }

  MetricTimeRangeValue(MetricTimeRangeValue metricTimeRangeValue) {
    this.startTime = metricTimeRangeValue.startTime;
    this.endTime = metricTimeRangeValue.endTime;
    this.value = metricTimeRangeValue.value;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[")
        .append(startTime).append("-").append(endTime)
        .append(":")
        .append(value)
        .append("]");
    return sb.toString();
  }
}
