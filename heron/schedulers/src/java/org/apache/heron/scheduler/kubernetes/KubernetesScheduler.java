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

package org.apache.heron.scheduler.kubernetes;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;

import com.google.common.base.Optional;

import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.UpdateTopologyManager;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.IScalable;
import org.apache.heron.spi.scheduler.IScheduler;


public class KubernetesScheduler implements IScheduler, IScalable {
  private static final Logger LOG = Logger.getLogger(KubernetesScheduler.class.getName());

  private Config configuration;
  private Config runtimeConfiguration;
  private KubernetesController controller;
  private UpdateTopologyManager updateTopologyManager;

  protected KubernetesController getController() {
    return new AppsV1Controller(configuration, runtimeConfiguration);
  }

  @Override
  public void initialize(Config config, Config runtime) {
    // validate the topology name before moving forward
    if (!topologyNameIsValid(Runtime.topologyName(runtime))) {
      throw new RuntimeException(getInvalidTopologyNameMessage(Runtime.topologyName(runtime)));
    }

    // validate that the image pull policy has been set correctly
    if (!imagePullPolicyIsValid(KubernetesContext.getKubernetesImagePullPolicy(config))) {
      throw new RuntimeException(
          getInvalidImagePullPolicyMessage(KubernetesContext.getKubernetesImagePullPolicy(config))
      );
    }

    final Config.Builder builder = Config.newBuilder()
        .putAll(config);
    if (config.containsKey(Key.TOPOLOGY_BINARY_FILE)) {
      builder.put(Key.TOPOLOGY_BINARY_FILE,
          FileUtils.getBaseName(Context.topologyBinaryFile(config)));
    }

    this.configuration = builder.build();
    this.runtimeConfiguration = runtime;
    this.controller = getController();
    this.updateTopologyManager =
        new UpdateTopologyManager(configuration, runtimeConfiguration,
            Optional.<IScalable>of(this));
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.getContainers().isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    LOG.info("Submitting topology to Kubernetes");

    return controller.submit(packing);
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new LinkedList<>();
    String kubernetesPodsLink = KubernetesContext.getSchedulerURI(configuration)
        + KubernetesConstants.JOB_LINK;
    jobLinks.add(kubernetesPodsLink);
    return jobLinks;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return controller.killTopology();
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    final int appId = request.getContainerIndex();
    return controller.restart(appId);
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

  /**
   * Add containers for a scale-up event from an update command
   *
   * @param containersToAdd the list of containers that need to be added
   *
   * NOTE: Due to the mechanics of Kubernetes pod creation, each container must be created on
   * a one-by-one basis. If one container out of many containers to be deployed failed, it will
   * leave the topology in a bad state.
   *
   * TODO (jrcrawfo) -- (https://github.com/apache/incubator-heron/issues/1981)
   */
  @Override
  public Set<PackingPlan.ContainerPlan>
      addContainers(Set<PackingPlan.ContainerPlan> containersToAdd) {
    controller.addContainers(containersToAdd);
    return containersToAdd;
  }

  /**
   * Remove containers for a scale-down event from an update command
   *
   * @param containersToRemove the list of containers that need to be removed
   *
   * NOTE: Due to the mechanics of Kubernetes pod removal, each container must be removed on
   * a one-by-one basis. If one container out of many containers to be removed failed, it will
   * leave the topology in a bad state.
   *
   * TODO (jrcrawfo) -- (https://github.com/apache/incubator-heron/issues/1981)
   */
  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    controller.removeContainers(containersToRemove);
  }

  static boolean topologyNameIsValid(String topologyName) {
    final Matcher matcher = KubernetesConstants.VALID_POD_NAME_REGEX.matcher(topologyName);
    return matcher.matches();
  }

  static boolean imagePullPolicyIsValid(String imagePullPolicy) {
    if (imagePullPolicy == null || imagePullPolicy.isEmpty()) {
      return true;
    }
    return KubernetesConstants.VALID_IMAGE_PULL_POLICIES.contains(imagePullPolicy);
  }

  private static String getInvalidTopologyNameMessage(String topologyName) {
    return String.format("Invalid topology name: \"%s\": "
        + "topology names in kubernetes must consist of lower case alphanumeric "
        + "characters, '-' or '.', and must start and end with an alphanumeric "
        + "character.", topologyName);
  }

  private static String getInvalidImagePullPolicyMessage(String policy) {
    return String.format("Invalid image pull policy: \"%s\": image pull polices must be one of "
            + " %s Defaults to Always if :latest tag is specified, or IfNotPresent otherwise.",
        policy, KubernetesConstants.VALID_IMAGE_PULL_POLICIES.toString());
  }
}
