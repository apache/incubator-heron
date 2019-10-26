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

import java.time.Duration;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey;
import org.apache.heron.healthmgr.policy.ToggleablePolicy;

public class HealthPolicyConfig {
  public static final String CONF_TOPOLOGY_NAME = "TOPOLOGY_NAME";
  public static final String CONF_METRICS_SOURCE_URL = "METRICS_SOURCE_URL";
  public static final String CONF_METRICS_SOURCE_TYPE = "METRICS_SOURCE_TYPE";
  public static final String CONF_POLICY_ID = "POLICY_ID";

  public static final String CONF_POLICY_MODE_DEACTIVATED = "deactivated";
  public static final String CONF_POLICY_MODE_ACTIVATED = "activated";

  private static final Logger LOG = Logger.getLogger(HealthPolicyConfig.class.getName());
  private final Map<String, Object> configs;

  HealthPolicyConfig(Map<String, Object> configs) {
    this.configs = configs;
    LOG.info("Health Policy Configuration:" + configs);
  }

  String getPolicyClass() {
    return (String) configs.get(PolicyConfigKey.HEALTH_POLICY_CLASS.key());
  }

  public ToggleablePolicy.PolicyMode getPolicyMode() {
    String configKey = PolicyConfigKey.HEALTH_POLICY_MODE.key();
    if (configs.containsKey(configKey)) {
      String val = (String) configs.get(PolicyConfigKey.HEALTH_POLICY_MODE.key());
      if (CONF_POLICY_MODE_DEACTIVATED.equals(val)) {
        return ToggleablePolicy.PolicyMode.deactivated;
      } else if (CONF_POLICY_MODE_ACTIVATED.equals(val)) {
        return ToggleablePolicy.PolicyMode.activated;
      } else {
        LOG.warning("unknown policy mode config " + val);
      }
    }
    return ToggleablePolicy.PolicyMode.activated;
  }

  public Duration getInterval() {
    return Duration.ofMillis((int) configs.get(PolicyConfigKey.HEALTH_POLICY_INTERVAL_MS.key()));
  }

  public Object getConfig(String configName) {
    return getConfig(configName, null);
  }

  public Object getConfig(String configName, Object defaultValue) {
    Object value = configs.get(configName);
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  @Override
  public String toString() {
    return configs.toString();
  }
}
