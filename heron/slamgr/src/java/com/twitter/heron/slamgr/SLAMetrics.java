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


package com.twitter.heron.slamgr;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

//Defines all the metrics that needs to be sent to SLA process
public class SLAMetrics {
  private static final Logger LOG = Logger.getLogger(SLAMetrics.class.getName());

  // map from metric prefix to its aggregation form
  private Map<String, MetricAggregationType> metrics_prefixes_;

  SLAMetrics(String sinks_filename) {
    // read config file and fill in 'Map<String, String> metrics'

    // mock config for prototype
    Map<String, String> metric = new HashMap<>();
    metric.put("__emit-count", "SUM");
    metric.put("__execute-count", "SUM");
    metric.put("__fail-count", "SUM");
    metric.put("__ack-count", "SUM");
    metric.put("__complete-latency", "AVG");
    metric.put("__execute-latency", "AVG");
    metric.put("__process-latency", "AVG");
    metric.put("__jvm-uptime-secs", "LAST");
    metric.put("__jvm-process-cpu-load", "LAST");
    metric.put("__jvm-memory-used-mb", "LAST");
    metric.put("__jvm-memory-mb-total", "LAST");
    metric.put("__jvm-gc-collection-time-ms", "LAST");
    metric.put("__server/__time_spent_back_pressure_initiated", "SUM");
    metric.put("__time_spent_back_pressure_by_compid", "SUM");

    InitSLAMetrics(metric);
  }


  public void InitSLAMetrics(Map<String, String> metrics) {
    for (Map.Entry<String, String> e : metrics.entrySet()) {
      metrics_prefixes_.put(e.getKey(), TranslateFromString(e.getValue()));
    }
  }

  public boolean IsTMasterMetric(String _name) {
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
  enum MetricAggregationType {
    UNKNOWN(-1),
    SUM(0),
    AVG(1),
    LAST(2);  // We only care about the last value

    private static Map<Integer, MetricAggregationType> map = new HashMap<>();

    static {
      for (MetricAggregationType mat : MetricAggregationType.values()) {
        map.put(mat.type, mat);
      }
    }

    private int type;

    private MetricAggregationType(final int _type) {
      type = _type;
    }

    public static MetricAggregationType valueOf(int _type) {
      return map.get(_type);
    }

    public int intValue() {
      return type;
    }
  }

}
