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

package org.apache.heron.healthmgr;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.ConfigReader;

/**
 * {@link HealthPolicyConfigReader} reads health manager configurations from
 * <code>healthmgr.yaml</code> file. The config file is expected to have a list of policy ids and
 * configuration specific to each of the policies.
 * <p>
 * Sample configuration
 * <pre>
 * heron.class.health.policies:
 *   - dynamic-resource-allocation
 *
 * dynamic-resource-allocation:
 *   health.policy.class: "org.apache.heron.healthmgr.policy.DynamicResourceAllocationPolicy"
 *   health.policy.interval.ms: "120000"
 * </pre>
 */
public class HealthPolicyConfigReader {
  public enum PolicyConfigKey {
    CONF_FILE_NAME("healthmgr.yaml"),
    HEALTH_POLICIES("heron.class.health.policies"),
    HEALTH_POLICY_MODE("health.policy.mode"),
    HEALTH_POLICY_CLASS("health.policy.class"),
    HEALTH_POLICY_INTERVAL_MS("health.policy.interval.ms"),
    CONF_SENSOR_DURATION_SUFFIX(".duration"),
    METRIC_SOURCE_TYPE("heron.healthmgr.metricsource.type"),
    METRIC_SOURCE_URL("heron.healthmgr.metricsource.url");

    private String key;

    PolicyConfigKey(String name) {
      this.key = name;
    }

    public String key() {
      return key;
    }
  }

  private final Map<String, Map<String, Object>> configs = new HashMap<>();
  private String configFilename;

  HealthPolicyConfigReader(String filename) throws FileNotFoundException {
    this.configFilename = filename;
  }

  @SuppressWarnings("unchecked")
  public void loadConfig() {
    Map<String, Object> ret = readConfigFromFile(configFilename);
    for (String id : TypeUtils.getListOfStrings(ret.get(PolicyConfigKey.HEALTH_POLICIES.key))) {
      configs.put(id, (Map<String, Object>) ret.get(id));
    }
  }

  @VisibleForTesting
  Map<String, Object> readConfigFromFile(String filename) {
    return ConfigReader.loadFile(filename);
  }

  List<String> getPolicyIds() {
    return new ArrayList<>(configs.keySet());
  }

  Map<String, Object> getPolicyConfig(String policyId) {
    return configs.get(policyId);
  }

  @Override
  public String toString() {
    return "HealthPolicyConfigReader {configFilename=" + configFilename + "}";
  }
}
