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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.ShellUtils;

/**
 * Implementation of AuroraController that shells out to the Aurora CLI to control the Aurora
 * scheduler workflow of a topology.
 */
class AuroraCLIController implements AuroraController {
  private static final Logger LOG = Logger.getLogger(AuroraCLIController.class.getName());

  private final String jobSpec;
  private final boolean isVerbose;
  private final String auroraFilename;

  AuroraCLIController(
      String jobName,
      String cluster,
      String role,
      String env,
      String auroraFilename,
      boolean isVerbose) {
    this.auroraFilename = auroraFilename;
    this.isVerbose = isVerbose;
    this.jobSpec = String.format("%s/%s/%s/%s", cluster, role, env, jobName);
  }

  @Override
  public boolean createJob(Map<AuroraField, String> bindings) {
    List<String> auroraCmd =
        new ArrayList<>(Arrays.asList("aurora", "job", "create", "--wait-until", "RUNNING"));

    for (AuroraField field : bindings.keySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", field, bindings.get(field)));
    }

    auroraCmd.add(jobSpec);
    auroraCmd.add(auroraFilename);

    if (isVerbose) {
      auroraCmd.add("--verbose");
    }

    return runProcess(auroraCmd);
  }

  // Kill an aurora job
  @Override
  public boolean killJob() {
    List<String> auroraCmd = new ArrayList<>(Arrays.asList("aurora", "job", "killall"));
    auroraCmd.add(jobSpec);

    appendAuroraCommandOptions(auroraCmd, isVerbose);

    return runProcess(auroraCmd);
  }

  // Restart an aurora job
  @Override
  public boolean restart(Integer containerId) {
    List<String> auroraCmd = new ArrayList<>(Arrays.asList("aurora", "job", "restart"));
    if (containerId != null) {
      auroraCmd.add(String.format("%s/%d", jobSpec, containerId));
    } else {
      auroraCmd.add(jobSpec);
    }

    appendAuroraCommandOptions(auroraCmd, isVerbose);

    return runProcess(auroraCmd);
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    String instancesToKill = getInstancesIdsToKill(containersToRemove);
    //aurora job kill <cluster>/<role>/<env>/<name>/<instance_ids>
    List<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "kill", jobSpec + "/" + instancesToKill));
    LOG.info(String.format(
        "Killing %s aurora containers: %s", containersToRemove.size(), auroraCmd));
    if (!runProcess(auroraCmd)) {
      throw new RuntimeException("Failed to kill freed aurora instances: " + instancesToKill);
    }
  }

  @Override
  public void addContainers(Integer count) {
    //aurora job add <cluster>/<role>/<env>/<name>/<instance_id> <count>
    //clone instance 0
    List<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "add", "--wait-until", "RUNNING", jobSpec + "/0", count.toString()));
    LOG.info(String.format("Requesting %s new aurora containers %s", count, auroraCmd));
    if (!runProcess(auroraCmd)) {
      throw new RuntimeException("Failed to create " + count + " new aurora instances");
    }
  }

  // Utils method for unit tests
  @VisibleForTesting
  boolean runProcess(List<String> auroraCmd) {
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();
    int status =
        ShellUtils.runProcess(auroraCmd.toArray(new String[auroraCmd.size()]), stdout, stderr);

    if (status != 0) {
      LOG.severe(String.format(
          "Failed to run process. Command=%s, STDOUT=%s, STDERR=%s", auroraCmd, stdout, stderr));
    }
    return status == 0;
  }

  private static String getInstancesIdsToKill(Set<PackingPlan.ContainerPlan> containersToRemove) {
    StringBuilder ids = new StringBuilder();
    for (PackingPlan.ContainerPlan containerPlan : containersToRemove) {
      if (ids.length() > 0) {
        ids.append(",");
      }
      ids.append(containerPlan.getId());
    }
    return ids.toString();
  }

  // Static method to append verbose and batching options if needed
  private static void appendAuroraCommandOptions(List<String> auroraCmd, boolean isVerbose) {
    // Append verbose if needed
    if (isVerbose) {
      auroraCmd.add("--verbose");
    }

    // Append batch size.
    // Note that we can not use "--no-batching" since "restart" command does not accept it.
    // So we play a small trick here by setting batch size Integer.MAX_VALUE.
    auroraCmd.add("--batch-size");
    auroraCmd.add(Integer.toString(Integer.MAX_VALUE));
  }
}
