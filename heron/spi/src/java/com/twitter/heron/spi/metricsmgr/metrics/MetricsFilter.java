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

package com.twitter.heron.spi.metricsmgr.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A MetricsFilter which could be used to:
 * 1. Specify the metric-prefix or metric-name we need to keep
 * 2. Query whether a metric is needed by contains method
 * 3. Get the MetricAggregationType by getAggregationType method
 * 4. Filter the needed MetricsInfo
 */
public class MetricsFilter {
  private final Map<String, MetricAggregationType> prefixToType =
      new HashMap<String, MetricAggregationType>();

  public void setPrefixToType(String prefix, MetricAggregationType type) {
    prefixToType.put(prefix, type);
  }

  public void setMetricToType(String metricName, MetricAggregationType type) {
    setPrefixToType(metricName, type);
  }

  public Set<String> getMetricNames() {
    return prefixToType.keySet();
  }

  public boolean contains(String metricName) {
    for (String prefix : prefixToType.keySet()) {
      if (metricName.contains(prefix)) {
        return true;
      }
    }
    return false;
  }

  // Return an immutable view of filtered metrics
  public Iterable<MetricsInfo> filter(Iterable<MetricsInfo> metricsInfos) {
    List<MetricsInfo> metricsFiltered = new ArrayList<MetricsInfo>();
    for (MetricsInfo metricsInfo : metricsInfos) {
      if (contains(metricsInfo.getName())) {
        metricsFiltered.add(metricsInfo);
      }
    }

    return metricsFiltered;
  }

  public MetricAggregationType getAggregationType(String metricName) {
    for (String prefix : prefixToType.keySet()) {
      if (metricName.contains(prefix)) {
        return prefixToType.get(prefix);
      }
    }
    return MetricAggregationType.UNKNOWN;
  }

  public enum MetricAggregationType {
    UNKNOWN, SUM, AVG, LAST;
  }
}
