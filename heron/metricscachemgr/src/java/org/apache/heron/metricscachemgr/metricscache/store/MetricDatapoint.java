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

package org.apache.heron.metricscachemgr.metricscache.store;

/**
 * immutable metric datum with timestamp in store
 * TODO(huijun) object pool to avoid java gc
 */
public final class MetricDatapoint {
  private final long timestamp;
  // one data point, compatible with protobuf message from sink
  private final String value;

  public MetricDatapoint(long timestamp, String value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getValue() {
    return value;
  }

  /**
   * test if the timestamp in the given time window [start, end]
   *
   * @param start time window start
   * @param end time window end
   * @return boolean test result
   */
  public boolean inRange(long start, long end) {
    return start <= timestamp && timestamp <= end;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(").append(timestamp).append(", ").append(value).append(")");
    return sb.toString();
  }

}
