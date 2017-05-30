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

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthPolicyConfigProviderTest {
  @Test
  public void testPolicyConfigurationInitialization() throws Exception {
    List<String> policyIds = new ArrayList<>();
    policyIds.add("p1");
    policyIds.add("p2");

    Map<String, String> policy1 = new HashMap<>();
    policy1.put("p1c1", "v1");
    policy1.put("p1c2", "v2");
    Map<String, String> policy2 = new HashMap<>();
    policy2.put("p2c1", "v1");
    policy2.put("p2c2", "v2");

    Map<String, Object> yamlContent = new HashMap<>();
    yamlContent.put(HealthMgrConstants.HEALTH_POLICIES, policyIds);
    yamlContent.put("p1", policy1);
    yamlContent.put("p2", policy2);

    FileInputStream fin = mock(FileInputStream.class);
    Yaml yaml = mock(Yaml.class);
    when(yaml.load(fin)).thenReturn(yamlContent);

    HealthPolicyConfigProvider policiesConfig = new HealthPolicyConfigProvider(yaml, fin);
    assertEquals(policyIds, policiesConfig.getPolicyIds());

    assertEquals(policy1, policiesConfig.getPolicyConfig("p1"));
    assertEquals(policy2, policiesConfig.getPolicyConfig("p2"));
  }
}
