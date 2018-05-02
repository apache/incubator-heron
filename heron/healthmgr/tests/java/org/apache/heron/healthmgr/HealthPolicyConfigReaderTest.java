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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class HealthPolicyConfigReaderTest {
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
    yamlContent.put(PolicyConfigKey.HEALTH_POLICIES.key(), policyIds);
    yamlContent.put("p1", policy1);
    yamlContent.put("p2", policy2);

    HealthPolicyConfigReader policyConfigReader = new HealthPolicyConfigReader("configFile");
    HealthPolicyConfigReader spyConfigReader = spy(policyConfigReader);
    doReturn(yamlContent).when(spyConfigReader).readConfigFromFile("configFile");

    spyConfigReader.loadConfig();
    assertEquals(policyIds, spyConfigReader.getPolicyIds());

    assertEquals(policy1, spyConfigReader.getPolicyConfig("p1"));
    assertEquals(policy2, spyConfigReader.getPolicyConfig("p2"));
  }
}
