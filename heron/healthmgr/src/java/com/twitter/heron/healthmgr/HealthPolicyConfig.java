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

package com.twitter.heron.healthmgr;

import java.time.Duration;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey;

public class HealthPolicyConfig {
  private static final Logger LOG = Logger.getLogger(HealthPolicyConfig.class.getName());
  private final Map<String, Object> configs;

  HealthPolicyConfig(Map<String, Object> configs) {
    this.configs = configs;
    LOG.info("Health Policy Configuration:" + configs);
  }

  String getPolicyClass() {
    return (String) configs.get(PolicyConfigKey.HEALTH_POLICY_CLASS.key());
  }

  public Duration getInterval() {
    return Duration.ofMillis((int) configs.get(PolicyConfigKey.HEALTH_POLICY_INTERVAL.key()));
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
