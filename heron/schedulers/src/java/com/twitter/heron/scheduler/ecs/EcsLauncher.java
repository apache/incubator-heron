// Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.scheduler.ecs;

import java.util.logging.Logger;

import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IScheduler;

/**
 * Submit topology to Ecs.
 */
public class EcsLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(EcsLauncher.class.getName());

  private Config config;
  private Config runtime;

  @Override
  public void initialize(Config aConfig, Config aRuntime) {
    this.config = aConfig;
    this.runtime = aRuntime;
  }

  @Override
  public void close() {
    // Do nothing
  }

  @Override
  public boolean launch(PackingPlan packing) {
    LauncherUtils launcherUtils = LauncherUtils.getInstance();
    Config ytruntime = launcherUtils.createConfigWithPackingDetails(runtime, packing);
    return launcherUtils.onScheduleAsLibrary(config, ytruntime, getScheduler(), packing);
  }

  // Get EcsScheduler
  protected IScheduler getScheduler() {
    return new EcsScheduler();
  }
}
