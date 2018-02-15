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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.scheduler.TopologyUpdateRecoverableException;
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
  private final String env;
  private final String cluster;
  private final String role;

  AuroraCLIController(
      String jobName,
      String cluster,
      String role,
      String env,
      String auroraFilename,
      boolean isVerbose) {
    this.auroraFilename = auroraFilename;
    this.isVerbose = isVerbose;
    this.env = env;
    this.cluster = cluster;
    this.role = role;
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

    appendAuroraCommandOptions(auroraCmd, isVerbose);
    LOG.info(String.format(
        "Killing %s aurora containers: %s", containersToRemove.size(), auroraCmd));
    if (!runProcess(auroraCmd)) {
      throw new RuntimeException("Failed to kill freed aurora instances: " + instancesToKill);
    }
  }

  private JsonNode askAuroraQuota() throws JsonProcessingException, IOException {
    String roleSpec = cluster + "/" + role;
    List<String> auroraCmd =
        new ArrayList<>(Arrays.asList("aurora", "quota", "get", "--write-json", roleSpec));

    LOG.info(String.format("Query quota: %s", auroraCmd));
    StringBuilder stderr = new StringBuilder();
    if (!runProcess(auroraCmd, null, stderr)) {
      throw new RuntimeException("Failed to query quota for " + roleSpec);
    }

    if (stderr.length() <= 0) { // no container was added
      throw new RuntimeException("empty quota query output by Aurora");
    }

    String jsonStr = stderr.toString();
    LOG.info(String.format("Quota: %s", jsonStr));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(jsonStr);
    return actualObj;
  }

  private JsonNode askAuroraStatus() throws JsonProcessingException, IOException {
    List<String> auroraCmd = new ArrayList<>(Arrays.asList("aurora", "job", "status", jobSpec));

    LOG.info(String.format("Query resource per container: %s", auroraCmd));
    StringBuilder stderr = new StringBuilder();
    if (!runProcess(auroraCmd, null, stderr)) {
      throw new TopologyUpdateRecoverableException(
          "Failed to query resource per container " + jobSpec);
    }

    if (stderr.length() <= 0) { // no container was added
      throw new TopologyUpdateRecoverableException(
          "empty resource per container query output by Aurora");
    }

    String jsonStr = stderr.toString();
    LOG.info(String.format("Resource per container: %s", jsonStr));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(jsonStr);
    return actualObj;
  }

  private boolean hasEnoughQuota(JsonNode resource, Integer count)
      throws JsonProcessingException, IOException {
    // calc available quota
    JsonNode quota = askAuroraQuota();

    JsonNode quotaNode = quota.get("quota");
    long quotaDiskMb = quotaNode.get("diskMb").longValue();
    double quotaNumCpus = quotaNode.get("numCpus").doubleValue();
    long quotaRamMb = quotaNode.get("ramMb").longValue();

    JsonNode prodNode = quota.get("prodSharedConsumption");
    long prodDiskMb = prodNode.get("diskMb").longValue();
    double prodNumCpus = prodNode.get("numCpus").doubleValue();
    long prodRamMb = prodNode.get("ramMb").longValue();

    long availableDiskMb = quotaDiskMb - prodDiskMb;
    double availableNumCpus = quotaNumCpus - prodNumCpus;
    long availableRamMb = quotaRamMb - prodRamMb;

    // calc res for one container
    JsonNode containerNode =
        resource.get(0).get("active").get(0).get("assignedTask").get("task").get("resources");
    double containerNumCpus = containerNode.get("numCpus").doubleValue();
    long containerRamMb = containerNode.get("ramMb").longValue();
    long containerDiskMb = containerNode.get("diskMb").longValue();

    // check if res is enough
    if (containerNumCpus * count > availableNumCpus) {
      String msg =
          String.format("not enough cpu quota: %f cpu per container x %d container > %f available",
              containerNumCpus, count, availableNumCpus);
      throw new TopologyUpdateRecoverableException(msg);
    } else if (containerRamMb * count > availableRamMb) {
      String msg =
          String.format("not enough ram quota: %d ram per container x %d container > %d available",
              containerRamMb, count, availableRamMb);
      throw new TopologyUpdateRecoverableException(msg);
    } else if (containerDiskMb * count > availableDiskMb) {
      String msg = String.format(
          "not enough disk quota: %d disk per container x %d container > %d available",
          containerDiskMb, count, availableDiskMb);
      throw new TopologyUpdateRecoverableException(msg);
    }
    return true;
  }

  private boolean isProd(JsonNode jsonStatus) {
    String tier = jsonStatus.get(0).get("active").get(0).get("assignedTask").get("task").get("tier")
        .textValue();
    if ("preferred".equals(tier)) {
      return true;
    }
    return false;
  }

  @Override
  public Set<Integer> addContainers(Integer count) {
    try {
      JsonNode jsonStatus = askAuroraStatus();
      if (isProd(jsonStatus)) {
        if (!hasEnoughQuota(jsonStatus, count)) {
          throw new TopologyUpdateRecoverableException(
              "[Aurora quota exception] Aurora quota request is not satified");
        }
      }
    } catch (IOException e) {
      throw new TopologyUpdateRecoverableException("recoverable error before `aurora add` command",
          e);
    }

    // aurora job add <cluster>/<role>/<env>/<name>/<instance_id> <count>
    // clone instance 0
    List<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "add", "--wait-until", "RUNNING",
        jobSpec + "/0", count.toString(), "--verbose"));

    LOG.info(String.format("Requesting %s new aurora containers %s", count, auroraCmd));
    StringBuilder stderr = new StringBuilder();
    if (!runProcess(auroraCmd, null, stderr)) {
      throw new RuntimeException("Failed to create " + count + " new aurora instances");
    }

    if (stderr.length() <= 0) { // no container was added
      throw new RuntimeException("empty output by Aurora");
    }
    return extractContainerIds(stderr.toString());
  }

  private Set<Integer> extractContainerIds(String auroraOutputStr) {
    String pattern = "Querying instance statuses: [";
    int idx1 = auroraOutputStr.indexOf(pattern);
    if (idx1 < 0) { // no container was added
      LOG.info("stdout & stderr by Aurora " + auroraOutputStr);
      return new HashSet<Integer>();
    }
    idx1 += pattern.length();
    int idx2 = auroraOutputStr.indexOf("]", idx1);
    String containerIdStr = auroraOutputStr.substring(idx1, idx2);
    LOG.info("container IDs returned by Aurora " + containerIdStr);
    return Arrays.asList(containerIdStr.split(", "))
        .stream().map(x->Integer.valueOf(x)).collect(Collectors.toSet());
  }

  // Utils method for unit tests
  @VisibleForTesting
  boolean runProcess(List<String> auroraCmd, StringBuilder stdout, StringBuilder stderr) {
    int status =
        ShellUtils.runProcess(auroraCmd.toArray(new String[auroraCmd.size()]),
            stderr != null ? stderr : new StringBuilder());

    if (status != 0) {
      LOG.severe(String.format(
          "Failed to run process. Command=%s, STDOUT=%s, STDERR=%s", auroraCmd, stdout, stderr));
    }
    return status == 0;
  }

  // Utils method for unit tests
  @VisibleForTesting
  boolean runProcess(List<String> auroraCmd) {
    return runProcess(auroraCmd, null, null);
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
