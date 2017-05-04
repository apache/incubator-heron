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

package com.twitter.heron.scheduler.ecs;

//import java.io.File;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.logging.Level;
import java.util.logging.Logger;

//import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.scheduler.utils.LauncherUtils;
//import com.twitter.heron.scheduler.utils.Runtime;
//import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
//import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IScheduler;
//import com.twitter.heron.spi.utils.ShellUtils;

/**
 * Created by ananth on 4/18/17.
 */
public class EcsLauncher implements ILauncher {
  protected static final Logger LOG = Logger.getLogger(EcsLauncher.class.getName());

  private Config config;
  private Config runtime;

  private String ecsWorkingDirectory;
  private String topologyWorkingDirectory;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
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

  protected IScheduler getScheduler() {
    return new EcsScheduler();
  }
}
