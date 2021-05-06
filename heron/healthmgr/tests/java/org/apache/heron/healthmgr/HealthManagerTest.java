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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.google.inject.AbstractModule;
import com.microsoft.dhalion.api.IHealthPolicy;
import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.policy.HealthPolicyImpl;

import org.junit.Test;

import org.apache.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey;
import org.apache.heron.healthmgr.sensors.TrackerMetricsProvider;
import org.apache.heron.proto.scheduler.Scheduler.SchedulerLocation;
import org.apache.heron.scheduler.client.ISchedulerClient;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class HealthManagerTest {
  @Test
  public void testInitialize() throws Exception {
    String topologyName = "testTopology";
    Config config = Config.newBuilder()
        .put(Key.SCHEDULER_IS_SERVICE, true)
        .put(Key.TOPOLOGY_NAME, topologyName)
        .put(Key.ENVIRON, "environ")
        .put(Key.CLUSTER, "cluster")
        .build();

    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);

    SchedulerLocation schedulerLocation = SchedulerLocation.newBuilder()
        .setTopologyName(topologyName)
        .setHttpEndpoint("http://127.0.0.1")
        .build();
    when(adaptor.getSchedulerLocation(anyString())).thenReturn(schedulerLocation);

    HealthManagerMetrics publishingMetrics = mock(HealthManagerMetrics.class);
    AbstractModule baseModule = HealthManager
        .buildBaseModule("127.0.0.1", TrackerMetricsProvider.class.getName(), publishingMetrics);

    HealthManager healthManager = new HealthManager(config, baseModule);

    Map<String, Object> policy = new HashMap<>();
    policy.put(PolicyConfigKey.HEALTH_POLICY_CLASS.key(), TestPolicy.class.getName());
    policy.put("test-config", "test-value");

    String[] policyIds = {"policy"};

    HealthPolicyConfigReader policyConfigProvider = mock(HealthPolicyConfigReader.class);
    when(policyConfigProvider.getPolicyIds()).thenReturn(Arrays.asList(policyIds));
    when(policyConfigProvider.getPolicyConfig("policy")).thenReturn(policy);

    HealthManager spyHealthMgr = spy(healthManager);
    doReturn(adaptor).when(spyHealthMgr).createStateMgrAdaptor();
    doReturn(policyConfigProvider).when(spyHealthMgr).createPolicyConfigReader();

    spyHealthMgr.initialize();

    List<IHealthPolicy> healthPolicies = spyHealthMgr.getHealthPolicies();
    assertEquals(1, healthPolicies.size());
    TestPolicy healthPolicy = (TestPolicy) healthPolicies.iterator().next();
    assertNotNull(healthPolicy.schedulerClient);
    assertEquals(healthPolicy.stateMgrAdaptor, adaptor);
    assertNotNull(healthPolicy.metricsProvider);
    assertEquals(healthPolicy.config.getConfig("test-config"), "test-value");
  }

  static class TestPolicy extends HealthPolicyImpl {
    private HealthPolicyConfig config;
    private final ISchedulerClient schedulerClient;
    private final SchedulerStateManagerAdaptor stateMgrAdaptor;
    private final MetricsProvider metricsProvider;

    @Inject
    TestPolicy(HealthPolicyConfig config,
                      ISchedulerClient schedulerClient,
                      SchedulerStateManagerAdaptor stateMgrAdaptor,
                      MetricsProvider metricsProvider) {
      this.config = config;
      this.schedulerClient = schedulerClient;
      this.stateMgrAdaptor = stateMgrAdaptor;
      this.metricsProvider = metricsProvider;
    }
  }
}
