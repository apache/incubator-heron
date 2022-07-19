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

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.Resource;

import io.kubernetes.client.openapi.models.V1StatefulSet;

final class StatefulSet {
  private final Map<Type, IStatefulSetFactory> statefulsets = new HashMap<>();

  public enum Type {
    Executor,
    Manager
  }

  /**
   * Container class of all the Kubernetes cluster configurations. The methods contained within
   * <code>KubernetesController</code> cannot be accessed externally since it is an abstract class.
   */
  static final class Configs {
    private final String namespace;
    private final String topologyName;
    private final Config configuration;
    private final Config runtimeConfiguration;

    Configs(String namespace, Config configuration, Config runtimeConfiguration) {
      this.namespace = namespace;
      this.topologyName = Runtime.topologyName(runtimeConfiguration);
      this.configuration = configuration;
      this.runtimeConfiguration = runtimeConfiguration;
    }

    Config getConfiguration() {
      return configuration;
    }

    Config getRuntimeConfiguration() {
      return runtimeConfiguration;
    }

    String getNamespace() {
      return namespace;
    }

    String getTopologyName() {
      return topologyName;
    }
  }

  private StatefulSet() {
    statefulsets.put(Type.Executor, new ExecutorFactory());
    statefulsets.put(Type.Manager, new ManagerFactory());
  }

  interface IStatefulSetFactory {
    V1StatefulSet create(Configs configs, Resource containerResources, int numberOfInstances);
  }

  /**
   * Creates configured <code>Executor</code> or <code>Manager</code> <code>Stateful Set</code>.
   * @param type One of <code>Executor</code> or <code>Manager</code>
   * @param configs Cluster configuration information container.
   * @param containerResources The container system resource configurations.
   * @param numberOfInstances The container count.
   * @return Fully configured <code>Stateful Set</code> or <code>null</code> on invalid <code>type</code>.
   */
  V1StatefulSet create(Type type, Configs configs, Resource containerResources,
                       int numberOfInstances) {
    if (statefulsets.containsKey(type)) {
      return statefulsets.get(type).create(configs, containerResources, numberOfInstances);
    }
    return null;
  }

  static class ExecutorFactory implements IStatefulSetFactory {

    @Override
    public V1StatefulSet create(Configs configs, Resource containerResources,
                                int numberOfInstances) {
      return null;
    }
  }

  static class ManagerFactory implements IStatefulSetFactory {

    @Override
    public V1StatefulSet create(Configs configs, Resource containerResources,
                                int numberOfInstances) {
      return null;
    }
  }
}
