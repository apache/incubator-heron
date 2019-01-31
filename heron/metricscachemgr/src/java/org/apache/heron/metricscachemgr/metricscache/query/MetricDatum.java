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

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * immutable data bag for metric datum
 * metric locator: &lt;componentName, instanceId, metricName&gt;
 * metric value list: metricValue (use immutable getter)
 */
public final class MetricDatum {
  private final String componentName;
  private final String instanceId;
  private final String metricName;
  private final ImmutableList<MetricTimeRangeValue> metricValue;

  public MetricDatum(String componentName, String instanceId, String metricName,
                     List<MetricTimeRangeValue> metricValue) {
    this.componentName = componentName;
    this.instanceId = instanceId;
    this.metricName = metricName;
    this.metricValue = ImmutableList.copyOf(metricValue);
  }

  public String getComponentName() {
    return componentName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getMetricName() {
    return metricName;
  }

  public List<MetricTimeRangeValue> getMetricValue() {
    return metricValue;
  }
}
