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

package com.twitter.heron.scheduler.utils;

import java.net.URI;
import java.util.Properties;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public final class Runtime {


  private Runtime() {
  }

  public static String topologyId(Config runtime) {
    return runtime.getStringValue(Keys.topologyId());
  }

  public static String topologyName(Config runtime) {
    return runtime.getStringValue(Keys.topologyName());
  }

  public static String topologyClassPath(Config runtime) {
    return runtime.getStringValue(Keys.topologyClassPath());
  }

  public static TopologyAPI.Topology topology(Config runtime) {
    return (TopologyAPI.Topology) runtime.get(Keys.topologyDefinition());
  }

  public static URI topologyPackageUri(Config cfg) {
    return (URI) cfg.get(Keys.topologyPackageUri());
  }

  public static SchedulerStateManagerAdaptor schedulerStateManagerAdaptor(Config runtime) {
    return (SchedulerStateManagerAdaptor) runtime.get(Keys.schedulerStateManagerAdaptor());
  }

  public static ILauncher launcherClassInstance(Config runtime) {
    return (ILauncher) runtime.get(Keys.launcherClassInstance());
  }

  public static Shutdown schedulerShutdown(Config runtime) {
    return (Shutdown) runtime.get(Keys.schedulerShutdown());
  }

  public static String componentRamMap(Config runtime) {
    return runtime.getStringValue(Keys.componentRamMap());
  }

  public static String componentJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.componentJvmOpts());
  }

  public static String instanceJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.instanceJvmOpts());
  }

  public static Long numContainers(Config runtime) {
    return runtime.getLongValue(Keys.numContainers());
  }

  public static Properties schedulerProperties(Config runtime) {
    return (Properties) runtime.get(Keys.SCHEDULER_PROPERTIES);
  }
}
