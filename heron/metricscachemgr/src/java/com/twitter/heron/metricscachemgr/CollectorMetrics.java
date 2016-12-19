//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License


package com.twitter.heron.metricscachemgr;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.metricsmgr.MetricsSinksConfig;

//Defines all the metrics that needs to be sent to MetricColletor process
public class CollectorMetrics {
  public static final String METRICS_SINKS_TMASTER_SINK = "tmaster-sink";
  public static final String METRICS_SINKS_TMASTER_METRICS = "tmaster-metrics-type";
  private static final Logger LOG = Logger.getLogger(SLAMetrics.class.getName());
  // map from metric prefix to its aggregation form
  private Map<String, MetricAggregationType> metrics_prefixes_;

  public CollectorMetrics(String sinks_filename) {
    metrics_prefixes_ = new HashMap<>();

    // read config file
    MetricsSinksConfig sink_config = new MetricsSinksConfig(sinks_filename);
    Map<String, String> metric_types =
        (Map<String, String>) sink_config
            .getConfigForSink(METRICS_SINKS_TMASTER_SINK)
            .get(METRICS_SINKS_TMASTER_METRICS);
    InitCollectorMetrics(metric_types);
  }


  public void InitCollectorMetrics(Map<String, String> metrics) {
    for (Map.Entry<String, String> e : metrics.entrySet()) {
      metrics_prefixes_.put(e.getKey(), TranslateFromString(e.getValue()));
    }
  }

  public boolean IsCollectorMetric(String _name) {
    for (String k : metrics_prefixes_.keySet()) {
      if (_name.indexOf(k) == 0) return true;
    }
    return false;
  }

  public MetricAggregationType GetAggregationType(String _name) {
    for (Map.Entry<String, MetricAggregationType> e : metrics_prefixes_.entrySet()) {
      if (_name.indexOf(e.getKey()) == 0) {
        return e.getValue();
      }
    }
    return MetricAggregationType.UNKNOWN;
  }

  private MetricAggregationType TranslateFromString(String type) {
    if (type == "SUM") {
      return MetricAggregationType.SUM;
    } else if (type == "AVG") {
      return MetricAggregationType.AVG;
    } else if (type == "LAST") {
      return MetricAggregationType.LAST;
    } else {
      LOG.log(Level.SEVERE, "Unknown metrics type in metrics sinks " + type);
      return MetricAggregationType.UNKNOWN;
    }
  }

  // metric types associated with int value
//  public enum MetricAggregationType {
//    UNKNOWN(-1),
//    SUM(0),
//    AVG(1),
//    LAST(2);  // We only care about the last value
//
//    private static Map<Integer, MetricAggregationType> map = new HashMap<>();
//
//    static {
//      for (MetricAggregationType mat : MetricAggregationType.values()) {
//        map.put(mat.type, mat);
//      }
//    }
//
//    private int type;
//
//    private MetricAggregationType(final int _type) {
//      type = _type;
//    }
//
//    public static MetricAggregationType valueOf(int _type) {
//      return map.get(_type);
//    }
//
//    public int intValue() {
//      return type;
//    }
//  }

}
