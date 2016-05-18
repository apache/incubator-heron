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

package com.twitter.heron.scheduler.hpc;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;

/**
 * Schedules a Heron topology in a HPC cluster.
 * Uses sbatch command to allocate the resources and srun to run the heron processes.
 * Then uses scancel to cancel the running job.s
 */
public class HPCScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(HPCScheduler.class.getName());
  private Config config;
  private Config runtime;
  private HPCController controller;
  private String workingDirectory;

  public HPCScheduler() {
  }

  public HPCScheduler(String workingDirectory) {
    this.workingDirectory = workingDirectory;
  }

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
    this.controller = getController();

    // get the topology working directory
    this.workingDirectory = HPCContext.workingDirectory(config);
  }

  /**
   * Get a HPCControl basing on the config and runtime
   *
   * @return HPCControler
   */
  protected HPCController getController() {
    return new HPCController(Context.verbose(config));
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.containers.isEmpty()) {
      LOG.log(Level.SEVERE, "No container requested. Can't schedule");
      return false;
    }
    LOG.info("Launching topology in HPC scheduler");
    // 1 additional for the tmaster
    long containers = Runtime.numContainers(runtime) + 1;
    boolean jobCreated = controller.createJob(getHeronHPCPath(),
        HPCContext.executorSandboxBinary(this.config),
        getExecutorCommand(packing),
        this.workingDirectory, containers);
    if (!jobCreated) {
      LOG.log(Level.SEVERE, "Failed to create job");
    } else {
      LOG.log(Level.FINE, "Job created successfully");
    }
    return jobCreated;
  }

  @Override
  public List<String> getJobLinks() {
    return new ArrayList<>();
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    // get the slurm id
    String file = getJobIdFilePath();
    return controller.killJob(file);
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    return true;
  }

  protected String getJobIdFilePath() {
    return new File(workingDirectory, HPCContext.jobIdFile(config)).getPath();
  }

  protected String getHeronHPCPath() {
    return new File(Context.heronConf(config), HPCContext.hpcShellScript(config)).getPath();
  }

  protected String[] getExecutorCommand(PackingPlan packing) {
    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    String[] executorCmd = SchedulerUtils.executorCommandArgs(
        packing, this.config, this.runtime, freePorts);

    LOG.info("Executor command line: " + Arrays.toString(executorCmd));
    return executorCmd;
  }
}
