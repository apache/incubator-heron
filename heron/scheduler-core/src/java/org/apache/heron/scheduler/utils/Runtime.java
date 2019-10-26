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

package org.apache.heron.scheduler.utils;

import java.net.URI;
import java.util.Properties;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.scheduler.ILauncher;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public final class Runtime {


  private Runtime() {
  }

  public static String topologyId(Config runtime) {
    return runtime.getStringValue(Key.TOPOLOGY_ID);
  }

  public static String topologyName(Config runtime) {
    return runtime.getStringValue(Key.TOPOLOGY_NAME);
  }

  public static String topologyClassPath(Config runtime) {
    return runtime.getStringValue(Key.TOPOLOGY_CLASSPATH);
  }

  public static TopologyAPI.Topology topology(Config runtime) {
    return (TopologyAPI.Topology) runtime.get(Key.TOPOLOGY_DEFINITION);
  }

  public static URI topologyPackageUri(Config cfg) {
    return (URI) cfg.get(Key.TOPOLOGY_PACKAGE_URI);
  }

  public static SchedulerStateManagerAdaptor schedulerStateManagerAdaptor(Config runtime) {
    return (SchedulerStateManagerAdaptor) runtime.get(Key.SCHEDULER_STATE_MANAGER_ADAPTOR);
  }

  public static ILauncher launcherClassInstance(Config runtime) {
    return (ILauncher) runtime.get(Key.LAUNCHER_CLASS_INSTANCE);
  }

  public static Shutdown schedulerShutdown(Config runtime) {
    return (Shutdown) runtime.get(Key.SCHEDULER_SHUTDOWN);
  }

  public static String componentRamMap(Config runtime) {
    return runtime.getStringValue(Key.COMPONENT_RAMMAP);
  }

  public static String componentJvmOpts(Config runtime) {
    return runtime.getStringValue(Key.COMPONENT_JVM_OPTS_IN_BASE64);
  }

  public static String instanceJvmOpts(Config runtime) {
    return runtime.getStringValue(Key.INSTANCE_JVM_OPTS_IN_BASE64);
  }

  public static Long numContainers(Config runtime) {
    return runtime.getLongValue(Key.NUM_CONTAINERS);
  }

  public static Properties schedulerProperties(Config runtime) {
    return (Properties) runtime.get(Key.SCHEDULER_PROPERTIES);
  }
}
