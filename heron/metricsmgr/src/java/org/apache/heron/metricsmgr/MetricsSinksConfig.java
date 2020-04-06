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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import org.apache.heron.common.basics.TypeUtils;

@SuppressWarnings("unchecked")
public class MetricsSinksConfig {
  public static final String CONFIG_KEY_METRICS_SINKS = "sinks";
  public static final String CONFIG_KEY_CLASSNAME = "class";
  public static final String CONFIG_KEY_FLUSH_FREQUENCY_MS = "flush-frequency-ms";
  public static final String CONFIG_KEY_SINK_RESTART_ATTEMPTS = "sink-restart-attempts";
  public static final int DEFAULT_SINK_RESTART_ATTEMPTS = 0;

  private final Map<String, Map<String, Object>> sinksConfigs = new HashMap<>();

  public MetricsSinksConfig(String metricsSinksConfigFilename, String overrideConfigFilename)
      throws IOException {
    Map<Object, Object> allConfig = new HashMap<>();
    allConfig.putAll(readConfig(metricsSinksConfigFilename));
    allConfig.putAll(readConfig(overrideConfigFilename));

    if (allConfig.isEmpty()) {
      throw new RuntimeException("Missing required config 'sinks' for metrics manager");
    }

    for (String sinkId : TypeUtils.getListOfStrings(allConfig.get(CONFIG_KEY_METRICS_SINKS))) {
      sinksConfigs.put(sinkId, (Map<String, Object>) allConfig.get(sinkId));
    }
  }

  private Map<Object, Object> readConfig(String configFile) throws IOException {
    if (configFile == null) {
      return Collections.emptyMap();
    }

    Yaml yaml = new Yaml(new SafeConstructor());
    try (InputStream inputStream = new FileInputStream(configFile)) {
      return (Map<Object, Object>) yaml.load(inputStream);
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
