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

package com.twitter.heron.spi.utils;

import java.net.URI;
import java.util.Properties;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public final class Runtime {


  private Runtime() {
  }

  public static String topologyId(SpiCommonConfig runtime) {
    return runtime.getStringValue(Keys.topologyId());
  }

  public static String topologyName(SpiCommonConfig runtime) {
    return runtime.getStringValue(Keys.topologyName());
  }

  public static String topologyClassPath(SpiCommonConfig runtime) {
    return runtime.getStringValue(Keys.topologyClassPath());
  }

  public static TopologyAPI.Topology topology(SpiCommonConfig runtime) {
    return (TopologyAPI.Topology) runtime.get(Keys.topologyDefinition());
  }

  public static URI topologyPackageUri(SpiCommonConfig cfg) {
    return (URI) cfg.get(Keys.topologyPackageUri());
  }

  public static SchedulerStateManagerAdaptor schedulerStateManagerAdaptor(SpiCommonConfig runtime) {
    return (SchedulerStateManagerAdaptor) runtime.get(Keys.schedulerStateManagerAdaptor());
  }

  public static IPacking packingClassInstance(SpiCommonConfig runtime) {
    return (IPacking) runtime.get(Keys.packingClassInstance());
  }

  public static ILauncher launcherClassInstance(SpiCommonConfig runtime) {
    return (ILauncher) runtime.get(Keys.launcherClassInstance());
  }

  public static Shutdown schedulerShutdown(SpiCommonConfig runtime) {
    return (Shutdown) runtime.get(Keys.schedulerShutdown());
  }

  public static String componentRamMap(SpiCommonConfig runtime) {
    return runtime.getStringValue(Keys.componentRamMap());
  }

  public static String componentJvmOpts(SpiCommonConfig runtime) {
    return runtime.getStringValue(Keys.componentJvmOpts());
  }

  public static String instanceDistribution(SpiCommonConfig runtime) {
    return runtime.getStringValue(Keys.instanceDistribution());
  }

  public static String instanceJvmOpts(SpiCommonConfig runtime) {
    return runtime.getStringValue(Keys.instanceJvmOpts());
  }

  public static Long numContainers(SpiCommonConfig runtime) {
    return runtime.getLongValue(Keys.numContainers());
  }

  public static Properties schedulerProperties(SpiCommonConfig runtime) {
    return (Properties) runtime.get(Keys.SCHEDULER_PROPERTIES);
  }
}
