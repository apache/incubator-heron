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

package org.apache.heron.metricsmgr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.basics.TypeUtils;

public class MetricsSinksConfig {
  public static final String CONFIG_KEY_METRICS_SINKS = "sinks";
  public static final String CONFIG_KEY_CLASSNAME = "class";
  public static final String CONFIG_KEY_FLUSH_FREQUENCY_MS = "flush-frequency-ms";
  public static final String CONFIG_KEY_SINK_RESTART_ATTEMPTS = "sink-restart-attempts";
  public static final int DEFAULT_SINK_RESTART_ATTEMPTS = 0;

  private final Map<String, Map<String, Object>> sinksConfigs = new HashMap<>();

  @SuppressWarnings("unchecked")
  public MetricsSinksConfig(String metricsSinksConfigFilename, String overrideConfigFilename)
      throws FileNotFoundException {
    FileInputStream sinkConfigStream = new FileInputStream(new File(metricsSinksConfigFilename));
    FileInputStream overrideStream = new FileInputStream(new File(overrideConfigFilename));
    try {
      Yaml yaml = new Yaml();
      Map<Object, Object> sinkConfig = (Map<Object, Object>) yaml.load(sinkConfigStream);
      Map<Object, Object> overrideConfig = (Map<Object, Object>) yaml.load(overrideStream);

      if (sinkConfig == null) {
        throw new RuntimeException("Could not parse metrics-sinks config file");
      }

      if (overrideConfig == null) {
        throw new RuntimeException("Could not parse override config file");
      }

      Map<Object, Object> allConfig = new HashMap<>();
      allConfig.putAll(sinkConfig);
      allConfig.putAll(overrideConfig);

      for (String sinkId : TypeUtils.getListOfStrings(allConfig.get(CONFIG_KEY_METRICS_SINKS))) {
        sinksConfigs.put(sinkId, (Map<String, Object>) allConfig.get(sinkId));
      }
    } finally {
      SysUtils.closeIgnoringExceptions(sinkConfigStream);
      SysUtils.closeIgnoringExceptions(overrideStream);
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
