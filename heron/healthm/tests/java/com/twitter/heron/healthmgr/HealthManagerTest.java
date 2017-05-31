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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IHealthPolicy;
import com.microsoft.dhalion.api.MetricsProvider;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.policy.ThroughputSLAPolicy;
import com.twitter.heron.proto.scheduler.Scheduler.SchedulerLocation;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

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
        .setHttpEndpoint("http://localhost")
        .build();
    when(adaptor.getSchedulerLocation(anyString())).thenReturn(schedulerLocation);

    HealthManager healthManager = new HealthManager(config, "localhost");

    HashMap policyConfig = new HashMap<>();
    policyConfig.put(HealthMgrConstants.HEALTH_POLICY_CLASS, ThroughputSLAPolicy.class.getName());

    String[] policyIds = {"policy"};
    Map policy = new HashMap<>();
    policy.put(HealthMgrConstants.HEALTH_POLICY_CLASS, ThroughputSLAPolicy.class.getName());

    HealthPolicyConfigProvider policyConfigProvider = mock(HealthPolicyConfigProvider.class);
    when(policyConfigProvider.getPolicyIds()).thenReturn(Arrays.asList(policyIds));
    when(policyConfigProvider.getPolicyClass("policy")).thenReturn(TestPolicy.class.getName());
    when(policyConfigProvider.getPolicyConfig("policy")).thenReturn(policy);

    HealthManager spyHealthMgr = spy(healthManager);
    doReturn(adaptor).when(spyHealthMgr).createStateMgrAdaptor();
    doReturn(policyConfigProvider).when(spyHealthMgr).createPolicyConfigProvider();

    spyHealthMgr.initialize();

    List<IHealthPolicy> healthPolicies = spyHealthMgr.getHealthPolicies();
    assertEquals(1, healthPolicies.size());
    TestPolicy healthPolicy = (TestPolicy) healthPolicies.iterator().next();
    assertNotNull(healthPolicy.schedulerClient);
    assertEquals(healthPolicy.stateMgrAdaptor, adaptor);
    assertNotNull(healthPolicy.metricsProvider);
    assertEquals(healthPolicy.policyConfigProvider, policyConfigProvider);
    assertEquals(1, healthPolicy.initialized.get());
  }

  static class TestPolicy implements IHealthPolicy {
    private final ISchedulerClient schedulerClient;
    private final SchedulerStateManagerAdaptor stateMgrAdaptor;
    private final MetricsProvider metricsProvider;
    private final HealthPolicyConfigProvider policyConfigProvider;

    AtomicInteger initialized = new AtomicInteger(0);

    @Inject
    public TestPolicy(ISchedulerClient schedulerClient,
                      SchedulerStateManagerAdaptor stateMgrAdaptor,
                      MetricsProvider metricsProvider,
                      HealthPolicyConfigProvider policyConfigProvider) {
      this.schedulerClient = schedulerClient;
      this.stateMgrAdaptor = stateMgrAdaptor;
      this.metricsProvider = metricsProvider;
      this.policyConfigProvider = policyConfigProvider;
    }

    @Override
    public void initialize() {
      initialized.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
    }
  }
}
