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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;

public class HealthPolicyConfigProvider {
  private static final Logger LOG = Logger.getLogger(HealthPolicyConfigProvider.class.getName());
  private final Map<String, Map<String, String>> configs = new HashMap<>();

  public HealthPolicyConfigProvider(String filename) throws FileNotFoundException {
    this(new Yaml(), new FileInputStream(new File(filename)));
  }

  @SuppressWarnings("unchecked")
  public HealthPolicyConfigProvider(Yaml yaml, FileInputStream fin) throws FileNotFoundException {
    Map<Object, Object> ret;
    try {
      ret = (Map<Object, Object>) yaml.load(fin);

      if (ret == null) {
        throw new RuntimeException("Could not parse health policy config file");
      }
    } finally {
      SysUtils.closeIgnoringExceptions(fin);
    }

    for (String id : TypeUtils.getListOfStrings(ret.get(HealthMgrConstants.HEALTH_POLICIES))) {
      configs.put(id, (Map<String, String>) ret.get(id));
    }

    LOG.info("Loading Health Policies configuration:" + configs);
  }

  @Override
  public String toString() {
    return configs.toString();
  }

  public List<String> getPolicyIds() {
    return new ArrayList<>(configs.keySet());
  }

  public Map<String, String> getPolicyConfig(String policyId) {
    return configs.get(policyId);
  }

  public String getPolicyClass(String policyId) {
    return configs.get(policyId).get(HealthMgrConstants.HEALTH_POLICY_CLASS);
  }
}
