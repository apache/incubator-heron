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

package org.apache.heron.scheduler.aurora;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.UpdateTopologyManager;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.common.TokenSub;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.scheduler.IScalable;
import org.apache.heron.spi.scheduler.IScheduler;

public class AuroraScheduler implements IScheduler, IScalable {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  private Config config;
  private Config runtime;
  private AuroraController controller;
  private UpdateTopologyManager updateTopologyManager;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = Config.toClusterMode(mConfig);
    this.runtime = mRuntime;
    try {
      this.controller = getController();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe("AuroraController initialization failed " + e.getMessage());
    }
    this.updateTopologyManager =
        new UpdateTopologyManager(config, runtime, Optional.<IScalable>of(this));
  }

  /**
   * Get an AuroraController based on the config and runtime
   *
   * @return AuroraController
   */
  protected AuroraController getController()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Boolean cliController = config.getBooleanValue(Key.AURORA_CONTROLLER_CLASS);
    Config localConfig = Config.toLocalMode(this.config);
    if (cliController) {
      return new AuroraCLIController(
          Runtime.topologyName(runtime),
          Context.cluster(localConfig),
          Context.role(localConfig),
          Context.environ(localConfig),
          AuroraContext.getHeronAuroraPath(localConfig),
          Context.verbose(localConfig));
    } else {
      return new AuroraHeronShellController(
          Runtime.topologyName(runtime),
          Context.cluster(localConfig),
          Context.role(localConfig),
          Context.environ(localConfig),
          AuroraContext.getHeronAuroraPath(localConfig),
          Context.verbose(localConfig),
          localConfig);
    }
  }

  @Override
  public void close() {
    if (updateTopologyManager != null) {
      updateTopologyManager.close();
    }
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.getContainers().isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    LOG.info("Launching topology in aurora");

    // Align the cpu, RAM, disk to the maximal one, and set them to ScheduledResource
    PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtime), updatedPackingPlan,
        Runtime.schedulerStateManagerAdaptor(runtime));

    // Use the ScheduledResource to create aurora properties
    // the ScheduledResource is guaranteed to be set after calling
    // cloneWithHomogeneousScheduledResource in the above code
    Resource containerResource =
        updatedPackingPlan.getContainers().iterator().next().getScheduledResource().get();
    Map<AuroraField, String> auroraProperties = createAuroraProperties(containerResource);
    Map<String, String> extraProperties = createExtraProperties(containerResource);

    return controller.createJob(auroraProperties, extraProperties);
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new ArrayList<>();

    //Only the aurora job page is returned
    String jobLinkFormat = AuroraContext.getJobLinkTemplate(config);
    if (jobLinkFormat != null && !jobLinkFormat.isEmpty()) {
      String jobLink = TokenSub.substitute(config, jobLinkFormat);
      jobLinks.add(jobLink);
    }

    return jobLinks;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    // The aurora service can be unavailable or unstable for a while,
    // we will try to kill the job with multiple attempts
    int attempts = AuroraContext.getJobMaxKillAttempts(config);
    long retryIntervalMs = AuroraContext.getJobKillRetryIntervalMs(config);
    LOG.info("Will try " + attempts + " attempts at interval: " + retryIntervalMs + " ms");

    // First attempt
    boolean res = controller.killJob();
    attempts--;

    // Failure retry
    while (!res && attempts > 0) {
      LOG.warning("Failed to kill the topology. Will retry in " + retryIntervalMs + " ms...");
      Utils.sleep(retryIntervalMs);

      // Retry the killJob()
      res = controller.killJob();
      attempts--;
    }

    return res;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    Integer containerId = null;
    if (request.getContainerIndex() != -1) {
      containerId = request.getContainerIndex();
    }
    return controller.restart(containerId);
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    try {
      updateTopologyManager.updateTopology(
          request.getCurrentPackingPlan(), request.getProposedPackingPlan());
    } catch (ExecutionException | InterruptedException e) {
      LOG.log(Level.SEVERE, "Could not update topology for request: " + request, e);
      return false;
    }
    return true;
  }

  private static final String CONFIRMED_YES = "y";
  boolean hasConfirmedWithUser(int newContainerCount) {
    LOG.info(String.format("After update there will be %d more containers. "
        + "Please make sure there are sufficient resources to update this job. "
        + "Continue update? [y/N]: ", newContainerCount));
    Scanner scanner = new Scanner(System.in);
    String userInput = scanner.nextLine();
    return CONFIRMED_YES.equalsIgnoreCase(userInput);
  }

  @Override
  public Set<PackingPlan.ContainerPlan> addContainers(
      Set<PackingPlan.ContainerPlan> containersToAdd) {
    Set<PackingPlan.ContainerPlan> remapping = new HashSet<>();
    if ("prompt".equalsIgnoreCase(Context.updatePrompt(config))
        && !hasConfirmedWithUser(containersToAdd.size())) {
      LOG.warning("Scheduler updated topology canceled.");
      return remapping;
    }

    // Do the actual containers adding
    LinkedList<Integer> newAddedContainerIds = new LinkedList<>(
        controller.addContainers(containersToAdd.size()));
    if (newAddedContainerIds.size() != containersToAdd.size()) {
      throw new RuntimeException(
          "Aurora returned different container count " + newAddedContainerIds.size()
          + "; input count was " + containersToAdd.size());
    }
    // Do the remapping:
    // use the `newAddedContainerIds` to replace the container id in the `containersToAdd`
    for (PackingPlan.ContainerPlan cp : containersToAdd) {
      PackingPlan.ContainerPlan newContainerPlan =
          new PackingPlan.ContainerPlan(
              newAddedContainerIds.pop(), cp.getInstances(),
              cp.getRequiredResource(), cp.getScheduledResource().orNull());
      remapping.add(newContainerPlan);
    }
    LOG.info("The remapping structure: " + remapping);
    return remapping;
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    controller.removeContainers(containersToRemove);
  }

  protected Map<AuroraField, String> createAuroraProperties(Resource containerResource) {
    Map<AuroraField, String> auroraProperties = new HashMap<>();

    TopologyAPI.Topology topology = Runtime.topology(runtime);

    auroraProperties.put(AuroraField.EXECUTOR_BINARY,
        Context.executorBinary(config));

    List<String> topologyArgs = new ArrayList<>();
    SchedulerUtils.addExecutorTopologyArgs(topologyArgs, config, runtime);
    String args = String.join(" ", topologyArgs);
    auroraProperties.put(AuroraField.TOPOLOGY_ARGUMENTS, args);

    auroraProperties.put(AuroraField.CLUSTER, Context.cluster(config));
    auroraProperties.put(AuroraField.ENVIRON, Context.environ(config));
    auroraProperties.put(AuroraField.ROLE, Context.role(config));
    auroraProperties.put(AuroraField.TOPOLOGY_NAME, topology.getName());

    auroraProperties.put(AuroraField.CPUS_PER_CONTAINER,
        Double.toString(containerResource.getCpu()));
    auroraProperties.put(AuroraField.DISK_PER_CONTAINER,
        Long.toString(containerResource.getDisk().asBytes()));
    auroraProperties.put(AuroraField.RAM_PER_CONTAINER,
        Long.toString(containerResource.getRam().asBytes()));

    auroraProperties.put(AuroraField.NUM_CONTAINERS,
        Integer.toString(1 + TopologyUtils.getNumContainers(topology)));

    // Job configuration attribute 'production' is deprecated.
    // Use 'tier' attribute instead
    // See: http://aurora.apache.org/documentation/latest/reference/configuration/#job-objects
    if ("prod".equals(Context.environ(config))) {
      auroraProperties.put(AuroraField.TIER, "preferred");
    } else {
      auroraProperties.put(AuroraField.TIER, "preemptible");
    }

    String heronCoreReleasePkgURI = Context.corePackageUri(config);
    String topologyPkgURI = Runtime.topologyPackageUri(runtime).toString();

    auroraProperties.put(AuroraField.CORE_PACKAGE_URI, heronCoreReleasePkgURI);
    auroraProperties.put(AuroraField.TOPOLOGY_PACKAGE_URI, topologyPkgURI);

    return auroraProperties;
  }

  protected Map<String, String> createExtraProperties(Resource containerResource) {
    Map<String, String> extraProperties = new HashMap<>();

    if (config.containsKey(Key.SCHEDULER_PROPERTIES)) {
      String[] meta = config.getStringValue(Key.SCHEDULER_PROPERTIES).split(",");
      extraProperties.put(AuroraContext.JOB_TEMPLATE, meta[0]);
      for (int idx = 1; idx < meta.length; idx++) {
        extraProperties.put("AURORA_METADATA_" + idx, meta[idx]);
      }
    }

    return extraProperties;
  }
}
