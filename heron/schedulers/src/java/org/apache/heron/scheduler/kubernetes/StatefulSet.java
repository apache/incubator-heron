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

import org.apache.heron.spi.packing.Resource;

import io.kubernetes.client.openapi.models.V1StatefulSet;

final class StatefulSet {
  private final Map<Type, IStatefulSetFactory> statefulsets = new HashMap<>();

  public enum Type {
    Executor,
    Manager
  }

  private StatefulSet() {
    statefulsets.put(Type.Executor, new ExecutorFactory());
    statefulsets.put(Type.Manager, new ManagerFactory());
  }

  interface IStatefulSetFactory {
    V1StatefulSet create(Resource containerResources, int numberOfInstances);
  }

  /**
   * Creates configured <code>Executor</code> or <code>Manager</code> <code>Stateful Set</code>.
   * @param type One of <code>Executor</code> or <code>Manager</code>
   * @param containerResources The container system resource configurations.
   * @param numberOfInstances The container count.
   * @return Fully configured <code>Stateful Set</code> or <code>null</code> on invalid <code>type</code>.
   */
  V1StatefulSet create(Type type, Resource containerResources, int numberOfInstances) {
    if (statefulsets.containsKey(type)) {
      return statefulsets.get(type).create(containerResources, numberOfInstances);
    }
    return null;
  }

  static class ExecutorFactory implements IStatefulSetFactory {

    @Override
    public V1StatefulSet create(Resource containerResources, int numberOfInstances) {
      return null;
    }
  }

  static class ManagerFactory implements IStatefulSetFactory {

    @Override
    public V1StatefulSet create(Resource containerResources, int numberOfInstances) {
      return null;
    }
  }
}
