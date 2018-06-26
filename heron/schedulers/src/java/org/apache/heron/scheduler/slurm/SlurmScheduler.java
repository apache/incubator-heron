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

package org.apache.heron.scheduler.slurm;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.IScheduler;

/**
 * Schedules a Heron topology in a HPC cluster using the Slurm Scheduler.
 * Uses sbatch command to allocate the resources and srun to run the heron processes.
 * Then uses scancel to cancel the running job.s
 */
public class SlurmScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(SlurmScheduler.class.getName());
  private Config config;
  private Config runtime;
  private SlurmController controller;
  private String workingDirectory;

  public SlurmScheduler() {
  }

  public SlurmScheduler(String workingDirectory) {
    this.workingDirectory = workingDirectory;
  }

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = Config.toClusterMode(mConfig);
    this.runtime = mRuntime;
    this.controller = getController();

    // get the topology working directory
    this.workingDirectory = SlurmContext.workingDirectory(config);
  }

  /**
   * Get a SlurmControl basing on the config and runtime
   *
   * @return SlurmController
   */
  protected SlurmController getController() {
    return new SlurmController(Context.verbose(config));
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.getContainers().isEmpty()) {
      LOG.log(Level.SEVERE, "No container requested. Can't schedule");
      return false;
    }
    LOG.info("Launching topology in Slurm scheduler");
    long containers = Runtime.numContainers(runtime);
    boolean jobCreated = controller.createJob(getHeronSlurmPath(),
        SlurmContext.executorBinary(this.config),
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

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    LOG.severe("Topology onUpdate not implemented by this scheduler.");
    return false;
  }

  protected String getJobIdFilePath() {
    return new File(workingDirectory, SlurmContext.jobIdFile(config)).getPath();
  }

  protected String getHeronSlurmPath() {
    return new File(Context.heronConf(config), SlurmContext.slurmShellScript(config)).getPath();
  }

  protected String[] getExecutorCommand(PackingPlan packing) {
    Map<ExecutorPort, String> ports = new HashMap<>();
    for (ExecutorPort executorPort : ExecutorPort.getRequiredPorts()) {
      int port = SysUtils.getFreePort();
      if (port == -1) {
        throw new RuntimeException("Failed to find available ports for executor");
      }
      ports.put(executorPort, String.valueOf(port));
    }

    String[] executorCmd = SchedulerUtils.executorCommandArgs(this.config, this.runtime,
        ports, null);

    LOG.log(Level.FINE, "Executor command line: ", Arrays.toString(executorCmd));
    return executorCmd;
  }
}
