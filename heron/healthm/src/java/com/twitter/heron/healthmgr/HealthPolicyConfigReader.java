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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.config.ConfigReader;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;

public class HealthPolicyConfigReader {
  private final Map<String, Map<String, String>> configs = new HashMap<>();
  private String configFilename;

  public HealthPolicyConfigReader(String filename) throws FileNotFoundException {
    this.configFilename = filename;
  }

  @SuppressWarnings("unchecked")
  public void loadConfig() {
    Map<String, Object> ret = readConfigFromFile(configFilename);
    for (String id : TypeUtils.getListOfStrings(ret.get(HealthMgrConstants.HEALTH_POLICIES))) {
      configs.put(id, (Map<String, String>) ret.get(id));
    }
  }

  @VisibleForTesting
  protected Map<String, Object> readConfigFromFile(String filename) {
    return ConfigReader.loadFile(filename);
  }

  public List<String> getPolicyIds() {
    return new ArrayList<>(configs.keySet());
  }

  public Map<String, String> getPolicyConfig(String policyId) {
    return configs.get(policyId);
  }

  @Override
  public String toString() {
    return "HealthPolicyConfigReader{"
        + "configFilename='" + configFilename + '\''
        + '}';
  }
}
