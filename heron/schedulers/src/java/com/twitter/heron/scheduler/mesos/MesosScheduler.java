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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.mesos.framework.BaseContainer;
import com.twitter.heron.scheduler.mesos.framework.MesosFramework;
import com.twitter.heron.scheduler.mesos.framework.TaskUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.scheduler.IScheduler;

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

    String masterURI = MesosContext.getHeronMesosMasterUri(config);
    this.driver = getSchedulerDriver(masterURI, mesosFramework);

    startSchedulerDriver();
  }

  /**
   * Start the scheduler driver and wait it to get registered
   */
  protected void startSchedulerDriver() {
    // start the driver non-blocking,
    // since we need to set heron state after the scheduler driver is started.
    // Heron will block the main thread eventually
    driver.start();

    // Staging the Mesos Framework
    LOG.info("Waiting for Mesos Framework get registered");
    long timeout = MesosContext.getHeronMesosFrameworkStagingTimeoutMs(config);
    if (!mesosFramework.waitForRegistered(timeout, TimeUnit.MILLISECONDS)) {
      throw new RuntimeException("Failed to register with Mesos Master in time");
    }
  }

  /**
   * Waits for the driver to be stopped or aborted
   *
   * @param timeout maximum time waiting
   * @param unit time unit to wait
   */
  protected void joinSchedulerDriver(long timeout, TimeUnit unit) {
    // ExecutorService used to monitor whether close() completes in time
    ExecutorService service = Executors.newFixedThreadPool(1);
    final CountDownLatch closeLatch = new CountDownLatch(1);
    Runnable driverJoin = new Runnable() {
      @Override
      public void run() {
        driver.join();
        closeLatch.countDown();
      }
    };
    service.submit(driverJoin);

    LOG.info("Waiting for Mesos Driver got stopped");
    try {
      if (!closeLatch.await(timeout, unit)) {
        LOG.severe("Mesos Driver failed to stop in time.");
      } else {
        LOG.info("Mesos Driver stopped.");
      }
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Close latch thread is interrupted: ", e);
    }

    // Shutdown the ExecutorService
    service.shutdownNow();
  }

  @Override
  public void close() {
    // Stop the SchedulerDriver
    if (driver != null) {
      // Stop the SchedulerDriver
      if (this.mesosFramework.isTerminated()) {
        driver.stop();
      } else {
        driver.stop(true);
      }

      // Waits for the driver to be stopped or aborted,
      // i.e. waits for the message sent to Mesos Master
      long stopTimeoutInMs = MesosContext.getHeronMesosSchedulerDriverStopTimeoutMs(config);
      joinSchedulerDriver(stopTimeoutInMs, TimeUnit.MILLISECONDS);
    }

    // Will not kill the topology when close() is invoked,
    // since the lifecycle of Topology is independent from Scheduler
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

    return mesosFramework.createJob(jobDefinition);
  }

  @Override
  public List<String> getJobLinks() {
    Protos.FrameworkID frameworkID = mesosFramework.getFrameworkId();
    // FrameworkID should exist otherwise onSchedule(..) returned false directly earlier
    // So no need to null check

    // The job link's format
    String jobLink = String.format("%s/#/frameworks/%s",
        MesosContext.getHeronMesosMasterUri(config), frameworkID.getValue());

    List<String> jobLinks = new ArrayList<>();
    jobLinks.add(jobLink);
    return jobLinks;
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

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    LOG.severe("Topology onUpdate not implemented by this scheduler.");
    return false;
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

    // Fill in the resources requirement for this container
    fillResourcesRequirementForBaseContainer(container, containerIndex, packing);

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

  /**
   * Fill the the resources requirement, i.e. cpu, memory and disk for the given container.
   * This method changes the BaseContainer passed in.
   * <p>
   * Notice: Currently we just make every container homogeneous,
   * requiring maximum resources for every container.
   *
   * @param container the BaseContainer to fill value in
   * @param containerIndex the index of the container
   * @param packing the packing plan
   */
  protected void fillResourcesRequirementForBaseContainer(
      BaseContainer container, Integer containerIndex, PackingPlan packing) {
    PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
    Resource maxResourceContainer =
        updatedPackingPlan.getContainers().iterator().next().getRequiredResource();

    double cpu = 0;
    ByteAmount disk = ByteAmount.ZERO;
    ByteAmount mem = ByteAmount.ZERO;
    for (PackingPlan.ContainerPlan cp : packing.getContainers()) {
      Resource containerResource = cp.getRequiredResource();
      cpu = Math.max(cpu, containerResource.getCpu());
      disk = disk.max(containerResource.getDisk());
      mem = mem.max(containerResource.getRam());
    }
    container.cpu = maxResourceContainer.getCpu();
    // Convert them from bytes to MB
    container.diskInMB = maxResourceContainer.getDisk().asMegabytes();
    container.memInMB = maxResourceContainer.getRam().asMegabytes();
    container.ports = SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR;
  }
}
