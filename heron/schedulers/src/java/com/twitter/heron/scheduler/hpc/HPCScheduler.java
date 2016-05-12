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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.*;
import com.twitter.heron.spi.utils.Runtime;
import org.apache.commons.io.FilenameUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

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
  public void initialize(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;
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

  /**
   * Encode the JVM options
   *
   * @return encoded string
   */
  private String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  private String getJobIdFilePath() {
    return new File(workingDirectory, HPCContext.jobIdFile(config)).getPath();
  }

  private String getHeronHPCPath() {
    return new File(Context.heronConf(config), "slurm.sh").getPath();
  }

  private List<String> getExecutorCommand(PackingPlan packing) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    int port1 = NetworkUtils.getFreePort();
    int port2 = NetworkUtils.getFreePort();
    int port3 = NetworkUtils.getFreePort();
    int shellPort = NetworkUtils.getFreePort();
    int port4 = NetworkUtils.getFreePort();

    if (port1 == -1 || port2 == -1 || port3 == -1) {
      throw new RuntimeException("Could not find available ports to start topology");
    }

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
    command.add(formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    command.add(TopologyUtils.makeClassPath(topology, HPCContext.topologyJarFile(config)));
    command.add(Integer.toString(port1));
    command.add(Integer.toString(port2));
    command.add(Integer.toString(port3));
    command.add(HPCContext.systemConfigSandboxFile(config));
    command.add(TopologyUtils.formatRamMap(
        TopologyUtils.getComponentRamMap(topology, HPCContext.instanceRam(config))));
    command.add(formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    command.add(HPCContext.topologyPackageType(config));
    command.add(HPCContext.topologyJarFile(config));
    command.add(HPCContext.javaSandboxHome(config));
    command.add(Integer.toString(shellPort));
    // command.add(HPCContext.logSandboxDirectory(config));
    command.add(HPCContext.shellSandboxBinary(config));
    command.add(Integer.toString(port4));
    command.add(HPCContext.cluster(config));
    command.add(HPCContext.role(config));
    command.add(HPCContext.environ(config));
    command.add(HPCContext.instanceSandboxClassPath(config));
    command.add(HPCContext.metricsSinksSandboxFile(config));
    command.add("no_need_since_scheduler_is_started");
    command.add(Integer.toString(0));

    LOG.info("Current directory: " + System.getProperty("user.dir"));
    Path currentRelativePath = Paths.get("");
    String s = currentRelativePath.toAbsolutePath().toString();
    LOG.info("Current relative path is: " + s);

    LOG.info("Executor command line: " + command.toString());
    return command;
  }

  protected boolean isProduction() {
    // TODO (nlu): currently enforce environment to be "prod" for a Production job
    return "prod".equals(Context.environ(config));
  }
}
