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

package org.apache.heron.scheduler.kubernetes;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;

public class KubernetesControllerTest {

  private static final String K8S_URI = "http://k8s.uri:8080";
  private static final String NAMESPACE = "ns1";
  private static final String TOPOLOGY_NAME = "topology-name";

  private Config config = Config.newBuilder()
      .put(KubernetesContext.HERON_KUBERNETES_SCHEDULER_URI, K8S_URI).build();

  private Config configWithNamespace = Config.newBuilder().putAll(config)
      .put(KubernetesContext.HERON_KUBERNETES_SCHEDULER_NAMESPACE, NAMESPACE).build();

  private Config runtime = Config.newBuilder()
      .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME).build();

  @Test
  public void testControllerSetup() {
    final KubernetesController controller = create(config, runtime);
    Assert.assertEquals(K8S_URI, controller.getKubernetesUri());
    Assert.assertEquals("default", controller.getNamespace());
    Assert.assertEquals(TOPOLOGY_NAME, controller.getTopologyName());
  }

  @Test
  public void testControllerSetupWithNamespace() {
    final KubernetesController controller = create(configWithNamespace, runtime);
    Assert.assertEquals(K8S_URI, controller.getKubernetesUri());
    Assert.assertEquals(NAMESPACE, controller.getNamespace());
    Assert.assertEquals(TOPOLOGY_NAME, controller.getTopologyName());
  }

  private KubernetesController create(Config c, Config r) {
    return new KubernetesController(c, r) {
      @Override
      boolean submit(PackingPlan packingPlan) {
        return false;
      }

      @Override
      boolean killTopology() {
        return false;
      }

      @Override
      boolean restart(int shardId) {
        return false;
      }

      @Override
      public Set<PackingPlan.ContainerPlan>
          addContainers(Set<PackingPlan.ContainerPlan> containersToAdd) {
        return containersToAdd;
      }

      @Override
      public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {

      }
    };
  }
}
