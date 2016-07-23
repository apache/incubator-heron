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

package com.twitter.heron.metricsmgr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.basics.TypeUtils;

public class MetricsSinksConfig {
  public static final String CONFIG_KEY_METRICS_SINKS = "sinks";
  public static final String CONFIG_KEY_CLASSNAME = "class";
  public static final String CONFIG_KEY_FLUSH_FREQUENCY_MS = "flush-frequency-ms";
  public static final String CONFIG_KEY_SINK_RESTART_ATTEMPTS = "sink-restart-attempts";
  public static final int DEFAULT_SINK_RESTART_ATTEMPTS = 0;

  private final Map<String, Map<String, Object>> sinksConfigs = new HashMap<>();

  @SuppressWarnings("unchecked")
  public MetricsSinksConfig(String filename) throws FileNotFoundException {
    FileInputStream fin = new FileInputStream(new File(filename));
    try {
      Yaml yaml = new Yaml();
      Map<Object, Object> ret = (Map<Object, Object>) yaml.load(fin);

      if (ret == null) {
        throw new RuntimeException("Could not parse metrics-sinks config file");
      } else {
        for (String sinkId : TypeUtils.getListOfStrings(ret.get(CONFIG_KEY_METRICS_SINKS))) {
          sinksConfigs.put(sinkId, (Map<String, Object>) ret.get(sinkId));
        }
      }
    } finally {
      SysUtils.closeIgnoringExceptions(fin);
    }
  }

  public void setConfig(String key, Object value) {
    for (Map<String, Object> config : sinksConfigs.values()) {
      config.put(key, value);
    }
  }

  @Override
  public String toString() {
    return sinksConfigs.toString();
  }

  public int getNumberOfSinks() {
    return sinksConfigs.keySet().size();
  }

  public Map<String, Object> getConfigForSink(String sinkId) {
    return sinksConfigs.get(sinkId);
  }

  public List<String> getSinkIds() {
    return new ArrayList<>(sinksConfigs.keySet());
  }
}
