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

import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;

public class HealthPolicyConfig {
  private static final Logger LOG = Logger.getLogger(HealthPolicyConfig.class.getName());
  private final Map<String, String> configs;

  public HealthPolicyConfig(Map<String, String> configs) {
    this.configs = configs;
    LOG.info("Health Policy Configuration:" + configs);
  }

  public String getPolicyClass(String policyId) {
    return configs.get(HealthMgrConstants.HEALTH_POLICY_CLASS);
  }

  public long getInterval() {
    return Long.valueOf(configs.get(HealthMgrConstants.HEALTH_POLICY_INTERVAL));
  }

  public String getConfig(String configName) {
    return configs.get(configName);
  }

  @Override
  public String toString() {
    return configs.toString();
  }
}
