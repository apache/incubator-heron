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
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

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
    this.controller = new HPCController(Context.verbose(config));

    // get the topology working directory
    this.workingDirectory = HPCContext.workingDirectory(config);
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.containers.isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }
    LOG.info("Launching topology in HPC scheduler");
    // 1 additional for the tmaster
    int containers = packing.containers.size() + 1;
    boolean jobCreated = controller.createJob(getHeronHPCPath(),
        HPCContext.executorSandboxBinary(config),
        getExecutorCommand(packing), this.workingDirectory, containers);
    if (!jobCreated) {
      LOG.severe("Failed to create job");
    } else {
      LOG.info("Job created successfully");
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

  private String getJobIdFilePath() {
    return new File(workingDirectory, HPCContext.jobIdFile(config)).getPath();
  }

  private String getHeronHPCPath() {
    return new File(Context.heronConf(config), HPCContext.hpcShellScript(config)).getPath();
  }

  private List<String> getExecutorCommand(PackingPlan packing) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    int masterPort = freePorts.get(0);
    int tmasterControllerPort = freePorts.get(1);
    int tmasterStatsPort = freePorts.get(2);
    int shellPort = freePorts.get(3);
    int metricsmgrPort = freePorts.get(4);
    int schedulerPort = freePorts.get(5);

    List<String> command = new ArrayList<>();
    command.add(topology.getName());
    command.add(topology.getId());
    command.add(FilenameUtils.getName(HPCContext.topologyDefinitionFile(config)));
    command.add(TopologyUtils.packingToString(packing));
    command.add(HPCContext.stateManagerConnectionString(config));
    command.add(HPCContext.stateManagerRootPath(config));
    command.add(HPCContext.tmasterSandboxBinary(config));
    command.add(HPCContext.stmgrSandboxBinary(config));
    command.add(HPCContext.metricsManagerSandboxClassPath(config));
    command.add(SchedulerUtils.encodeJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    command.add(TopologyUtils.makeClassPath(topology, HPCContext.topologyJarFile(config)));
    command.add(Integer.toString(masterPort));
    command.add(Integer.toString(tmasterControllerPort));
    command.add(Integer.toString(tmasterStatsPort));
    command.add(HPCContext.systemConfigSandboxFile(config));
    command.add(TopologyUtils.formatRamMap(
        TopologyUtils.getComponentRamMap(topology, HPCContext.instanceRam(config))));
    command.add(SchedulerUtils.encodeJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    command.add(HPCContext.topologyPackageType(config));
    command.add(HPCContext.topologyJarFile(config));
    command.add(HPCContext.javaSandboxHome(config));
    command.add(Integer.toString(shellPort));
    command.add(HPCContext.shellSandboxBinary(config));
    command.add(Integer.toString(metricsmgrPort));
    command.add(HPCContext.cluster(config));
    command.add(HPCContext.role(config));
    command.add(HPCContext.environ(config));
    command.add(HPCContext.instanceSandboxClassPath(config));
    command.add(HPCContext.metricsSinksSandboxFile(config));

    String completeSchedulerProcessClassPath = new StringBuilder()
        .append(Context.schedulerSandboxClassPath(config)).append(":")
        .append(Context.packingSandboxClassPath(config)).append(":")
        .append(Context.stateManagerSandboxClassPath(config))
        .toString();

    command.add(completeSchedulerProcessClassPath);
    command.add(Integer.toString(schedulerPort));

    return command;
  }
}
