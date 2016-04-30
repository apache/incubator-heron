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

package com.twitter.heron.scheduler.aurora;

import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.SchedulerUtils;

/**
 * Launch topology locally to Aurora.
 */

public class AuroraLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  private Config config;
  private Config runtime;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean launch(PackingPlan packing) {
    return SchedulerUtils.onScheduleAsLibrary(config, runtime, getScheduler(), packing);
  }

  // Get AuroraScheduler
  protected IScheduler getScheduler() {
    return new AuroraScheduler();
  }
}
