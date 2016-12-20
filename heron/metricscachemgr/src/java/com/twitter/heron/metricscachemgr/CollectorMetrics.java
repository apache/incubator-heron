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

package com.twitter.heron.metricscachemgr;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.metricsmgr.MetricsSinksConfig;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter.MetricAggregationType;

//Defines all the metrics that needs to be sent to MetricColletor process
public class CollectorMetrics {
  public static final String METRICS_SINKS_TMASTER_SINK = "tmaster-sink";
  public static final String METRICS_SINKS_TMASTER_METRICS = "tmaster-metrics-type";
  private static final Logger LOG = Logger.getLogger(CollectorMetrics.class.getName());
  // map from metric prefix to its aggregation form
  private MetricsFilter metricsfilter = null;

  @SuppressWarnings("unchecked")
  public CollectorMetrics(String sinksFilename) throws FileNotFoundException {
    metricsfilter = new MetricsFilter();

    // read config file
    MetricsSinksConfig sinksConfig = new MetricsSinksConfig(sinksFilename);
    Map<String, Object> sinksTmaster = sinksConfig.getConfigForSink(METRICS_SINKS_TMASTER_SINK);
    Map<String, String> metricsType =
        (Map<String, String>) sinksTmaster.get(METRICS_SINKS_TMASTER_METRICS);
    for (Map.Entry<String, String> e : metricsType.entrySet()) {
      metricsfilter.setMetricToType(e.getKey(), TranslateFromString(e.getValue()));
    }
  }

  private MetricAggregationType TranslateFromString(String type) {
    if ("SUM".equals(type)) {
      return MetricAggregationType.SUM;
    } else if ("AVG".equals(type)) {
      return MetricAggregationType.AVG;
    } else if ("LAST".equals(type)) {
      return MetricAggregationType.LAST;
    } else {
      LOG.log(Level.SEVERE, "Unknown metrics type in metrics sinks " + type);
      return MetricAggregationType.UNKNOWN;
    }
  }

  public MetricsFilter getMetricsFilter() {
    return metricsfilter;
  }
}
