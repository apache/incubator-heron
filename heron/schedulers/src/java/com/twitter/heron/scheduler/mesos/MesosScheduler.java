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

package com.twitter.heron.scheduler.mesos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.mesos.framework.BaseContainer;
import com.twitter.heron.scheduler.mesos.framework.MesosFramework;
import com.twitter.heron.scheduler.mesos.framework.TaskUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;

/**
 * Schedule a topology to a mesos cluster
 */

public class MesosScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(MesosScheduler.class.getName());

  private Config config;
  private Config runtime;
  private MesosFramework mesosFramework;
  private SchedulerDriver driver;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
    this.mesosFramework = getMesosFramework();
  }

  @Override
  public void close() {
    // Stop the SchedulerDriver
    if (driver != null) {
      driver.stop();
    }
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    // Construct the jobDefinition
    Map<Integer, BaseContainer> jobDefinition = new HashMap<>();

    for (int containerIndex = 0;
         containerIndex < Runtime.numContainers(runtime);
         containerIndex++) {
      jobDefinition.put(containerIndex, getBaseContainer(containerIndex, packing));
    }

    String masterURI = MesosContext.getHeronMesosMasterUri(config);
    this.driver = getSchedulerDriver(masterURI, mesosFramework);

    // start the driver, non-blocking
    driver.start();

    return mesosFramework.createJob(jobDefinition);
  }

  @Override
  public List<String> getJobLinks() {
    return new ArrayList<>();
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return mesosFramework.killJob();
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    int containerId = request.getContainerIndex();
    return mesosFramework.restartJob(containerId);
  }

  protected MesosFramework getMesosFramework() {
    return new MesosFramework(config, runtime);
  }

  protected SchedulerDriver getSchedulerDriver(String masterURI, MesosFramework framework) {
    Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("heron_scheduler_" + Context.topologyName(config))
        .setCheckpoint(true);

    return new MesosSchedulerDriver(
        framework,
        frameworkBuilder.build(),
        masterURI);
  }


  /**
   * Get BaseContainer info.
   *
   * @param containerIndex container index to start
   * @param packing the PackingPlan
   * @return BaseContainer Info
   */
  protected BaseContainer getBaseContainer(Integer containerIndex, PackingPlan packing) {
    BaseContainer container = new BaseContainer();

    container.name = TaskUtils.getTaskNameForContainerIndex(containerIndex);

    container.runAsUser = Context.role(config);
    container.description = String.format("Container %d for topology %s",
        containerIndex, Context.topologyName(config));

    // We assume every container is homogeneous
    PackingPlan.Resource containerResource =
        packing.containers.values().iterator().next().resource;
    container.cpu = containerResource.cpu;
    container.disk = containerResource.disk / Constants.MB;
    container.mem = containerResource.ram / Constants.MB;
    container.ports = SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR;

    // Force running as shell
    container.shell = true;

    // Infinite retries
    container.retries = Integer.MAX_VALUE;

    // The dependencies for the container
    container.dependencies = new ArrayList<>();
    String topologyPath =
        Runtime.schedulerProperties(runtime).getProperty(Keys.topologyPackageUri());
    String heronCoreReleasePath = Context.corePackageUri(config);

    container.dependencies.add(topologyPath);
    container.dependencies.add(heronCoreReleasePath);
    return container;
  }
}
