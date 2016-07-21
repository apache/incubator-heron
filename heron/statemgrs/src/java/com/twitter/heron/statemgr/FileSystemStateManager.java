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

package com.twitter.heron.statemgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.WatchCallback;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.ShellUtils;

public abstract class FileSystemStateManager implements IStateManager {
  private static final Logger LOG = Logger.getLogger(FileSystemStateManager.class.getName());

  // Store the root address of the hierarchical file system
  protected String rootAddress;

  protected abstract ListenableFuture<Boolean> nodeExists(String path);

  protected abstract ListenableFuture<Boolean> deleteNode(String path);

  protected abstract <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                                         String path,
                                                                         Message.Builder builder);

  protected String getTMasterLocationDir() {
    return concatPath(rootAddress, "tmasters");
  }

  protected String getTopologyDir() {
    return concatPath(rootAddress, "topologies");
  }

  protected String getPackingPlanDir() {
    return concatPath(rootAddress, "packingplans");
  }

  protected String getPhysicalPlanDir() {
    return concatPath(rootAddress, "pplans");
  }

  protected String getExecutionStateDir() {
    return concatPath(rootAddress, "executionstate");
  }

  protected String getSchedulerLocationDir() {
    return concatPath(rootAddress, "schedulers");
  }

  protected String getTMasterLocationPath(String topologyName) {
    return concatPath(getTMasterLocationDir(), topologyName);
  }

  protected String getTopologyPath(String topologyName) {
    return concatPath(getTopologyDir(), topologyName);
  }

  protected String getPackingPlanPath(String topologyName) {
    return concatPath(getPackingPlanDir(), topologyName);
  }

  protected String getPhysicalPlanPath(String topologyName) {
    return concatPath(getPhysicalPlanDir(), topologyName);
  }

  protected String getExecutionStatePath(String topologyName) {
    return concatPath(getExecutionStateDir(), topologyName);
  }

  protected String getSchedulerLocationPath(String topologyName) {
    return concatPath(getSchedulerLocationDir(), topologyName);
  }

  @Override
  public void initialize(Config config) {
    this.rootAddress = Context.stateManagerRootPath(config);
    LOG.log(Level.FINE, "File system state manager root address: {0}", rootAddress);
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    return deleteNode(getTMasterLocationPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    return deleteNode(getSchedulerLocationPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    return deleteNode(getExecutionStatePath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    return deleteNode(getTopologyPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deletePackingPlan(String topologyName) {
    return deleteNode(getPackingPlanPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    return deleteNode(getPhysicalPlanPath(topologyName));
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getSchedulerLocationPath(topologyName),
        Scheduler.SchedulerLocation.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getTopologyPath(topologyName), TopologyAPI.Topology.newBuilder());
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getExecutionStatePath(topologyName),
        ExecutionEnvironment.ExecutionState.newBuilder());
  }

  @Override
  public ListenableFuture<PackingPlans.PackingPlan> getPackingPlan(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getPackingPlanPath(topologyName),
        PackingPlans.PackingPlan.newBuilder());
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getPhysicalPlanPath(topologyName),
        PhysicalPlans.PhysicalPlan.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getTMasterLocationPath(topologyName),
        TopologyMaster.TMasterLocation.newBuilder());
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    return nodeExists(getTopologyPath(topologyName));
  }

  private static String concatPath(String basePath, String appendPath) {
    return String.format("%s/%s", basePath, appendPath);
  }

  /**
   * Returns all information stored in the StateManager. This is a utility method used for debugging
   * while developing. To invoke, run:
   *
   *  bazel run heron/statemgrs/src/java:localfs-statemgr-unshaded -- \
   *    &lt;topology-name&gt; [new_instance_distribution]
   *
   * If a new_instance_distribution is provided, the instance distribution will be updated to
   * trigger a scaling event. For example:
   *
   *  bazel run heron/statemgrs/src/java:localfs-statemgr-unshaded -- \
   *    ExclamationTopology 1:word:3:0:exclaim1:2:0:exclaim1:1:0
   *
   */
  protected static void doMain(
      Class<? extends FileSystemStateManager> stateManagerClass, String[] args, Config config)
      throws ExecutionException, InterruptedException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    FileSystemStateManager stateManager =
        ReflectionUtils.<FileSystemStateManager>newInstance(stateManagerClass.getCanonicalName());

    if (args.length < 1) {
      throw new RuntimeException(String.format(
          "Usage: java %s <topology_name> - view state manager details for a topology",
          stateManagerClass.getCanonicalName()));
    }

    String topologyName = args[0];
    print("==> State Manager root path: %s", config.get(Keys.stateManagerRootPath()));

    stateManager.initialize(config);

    PackingPlans.PackingPlan existingPackingPlan = null;
    if (stateManager.isTopologyRunning(topologyName).get()) {
      print("==> Topology %s found", topologyName);
      print("==> ExecutionState:\n%s", stateManager.getExecutionState(null, topologyName).get());
      print("==> SchedulerLocation:\n%s",
          stateManager.getSchedulerLocation(null, topologyName).get());
      print("==> TMasterLocation:\n%s", stateManager.getTMasterLocation(null, topologyName).get());
      print("==> PhysicalPlan:\n%s", stateManager.getPhysicalPlan(null, topologyName).get());
      existingPackingPlan = stateManager.getPackingPlan(null, topologyName).get();
      print("==> PackingPlan:\n%s", existingPackingPlan);

      if (args.length == 2) {
        String proposedInstanceDistribution = args[1];
        adjustTopology(stateManager, topologyName,
            existingPackingPlan, proposedInstanceDistribution);
      }
    } else {
      print("==> Topology %s not found under %s",
          topologyName, config.get(Keys.stateManagerRootPath()));
    }
  }

  // Handles scaling the cluster out or in based on the proposedInstanceDistribution
  private static void adjustTopology(FileSystemStateManager stateManager,
                                     String topologyName,
                                     PackingPlans.PackingPlan existingPackingPlan,
                                     String proposedInstanceDistribution)
      throws ExecutionException, InterruptedException {
    Map<String, Integer> proposedChanges = parallelismChanges(
        existingPackingPlan.getInstanceDistribution(),
        proposedInstanceDistribution);

    PackingPlans.PackingPlan newPackingPlan = createPackingPlan(
        proposedInstanceDistribution,
        existingPackingPlan.getComponentRamDistribution());

    TopologyAPI.Topology topology = stateManager.getTopology(null, topologyName).get();
    for (Map.Entry<String, Integer> proposedChange : proposedChanges.entrySet()) {
      String componentName = proposedChange.getKey();
      Integer parallelism = proposedChange.getValue();
      print("Updating packing plan to change component %s to parallelism=%s", componentName,
          parallelism);

      topology = mergeTopology(topology, componentName, parallelism);
    }

    int existingContainerCount = parseInstanceDistributionContainers(
        existingPackingPlan.getInstanceDistribution()).length;
    int proposedContainerCount = parseInstanceDistributionContainers(
        proposedInstanceDistribution).length;
    Integer containerDelta = proposedContainerCount - existingContainerCount;

    assertTrue(proposedContainerCount < 1,
        "proposed instance distribution must have at least 1 container %s",
        proposedInstanceDistribution);

    // request new aurora resources if necessary. Once containers are allocated we must make the
    // changes to state manager quickly, otherwise aurora might penalize for thrashing on start-up
    if (containerDelta > 0) {
      //aurora job add smf1/billg/devel/ExclamationTopology/0 1
      List<String> auroraCmd =
          new ArrayList<>(Arrays.asList("aurora", "job", "add", "--wait-until",
              "RUNNING", getAuroraJobName(topologyName) + "/0", containerDelta.toString()));
      print("Requesting %s new aurora containers %s", containerDelta, auroraCmd);
      assertTrue(runProcess(auroraCmd), "Failed to create new aurora instances");
    }

    //update parallelism in topology since TMaster checks that Sum(parallelism) == Sum(instances)
    if (stateManager.nodeExists(stateManager.getTopologyPath(topologyName)).get()) {
      print("==> Deleted existing Topology: %s",
          stateManager.deleteTopology(topologyName).get());
    }

    print("==> Set Topology: %s", stateManager.setTopology(topology, topologyName).get());

    topology = stateManager.getTopology(null, topologyName).get();
    print("==> Got Topology: %s", topology);

    if (stateManager.nodeExists(stateManager.getPackingPlanPath(topologyName)).get()) {
      print("==> Deleted existing Topology: %s",
          stateManager.deletePackingPlan(topologyName).get());
    }
    stateManager.setPackingPlan(newPackingPlan, topologyName);
    existingPackingPlan = stateManager.getPackingPlan(null, topologyName).get();
    print("==> Updated PackingPlan:\n%s", existingPackingPlan);

    if (stateManager.nodeExists(stateManager.getPhysicalPlanPath(topologyName)).get()) {
      print("==> Deleted Physical Plan: %s", stateManager.deletePhysicalPlan(topologyName).get());
    }

    if (containerDelta < 0) {
      String instancesToKill = getInstancesIdsToKill(existingContainerCount, -containerDelta);
      //aurora job kill smf1/billg/devel/ExclamationTopology/3,4,5
      List<String> auroraCmd =
          new ArrayList<>(Arrays.asList("aurora", "job", "kill", "--wait-until",
              "RUNNING", getAuroraJobName(topologyName) + "/" + instancesToKill));
      print("Killing %s aurora containers %s", -containerDelta, auroraCmd);
      assertTrue(runProcess(auroraCmd),
          "Failed to kill freed aurora instances %s", instancesToKill);
    }
  }

  private static String getAuroraJobName(String topologyName) {
    //TODO: don't assume smf1 and devel
    return String.format("smf1/%s/devel/%s", System.getProperty("user.name"), topologyName);
  }

  private static String getInstancesIdsToKill(int totalCount, int numToKill) {
    StringBuilder ids = new StringBuilder();
    for (int id = totalCount - numToKill + 1; id < totalCount; id++) {
      if (ids.length() > 0) {
        ids.append(",");
      }
      ids.append(id);
    }
    return ids.toString();
  }

  // Given the existing and proposed instance distribution, verify the proposal and return only
  // the changes being requested.
  private static Map<String, Integer> parallelismChanges(String existingInstanceDistribution,
                                                         String proposedInstanceDistribution) {
    Map<String, Integer> existingParallelism =
        parseInstanceDistribution(existingInstanceDistribution);
    Map<String, Integer> proposedParallelism =
        parseInstanceDistribution(proposedInstanceDistribution);
    Map<String, Integer> parallelismChanges = new HashMap<>();

    for (String componentName : proposedParallelism.keySet()) {
      Integer newParallelism = proposedParallelism.get(componentName);
      assertTrue(existingParallelism.containsKey(componentName),
          "key %s in proposed instance distribution %s not found in "
              + "current instance distribution %s",
          componentName, proposedInstanceDistribution, existingInstanceDistribution);
      assertTrue(newParallelism > 0,
          "Non-positive parallelism (%s) for component %s found in instance distribution %s",
          newParallelism, componentName, proposedInstanceDistribution);

      if (!newParallelism.equals(existingParallelism.get(componentName))) {
        parallelismChanges.put(componentName, newParallelism);
      }
    }
    return parallelismChanges;
  }

  // given a string like "1:word:3:0:exclaim1:2:0:exclaim1:1:0,2:exclaim1:4:0" returns a map of
  // { word -> 1, explain1 -> 3 }
  private static Map<String, Integer> parseInstanceDistribution(String instanceDistribution) {
    Map<String, Integer> componentParallelism = new HashMap<>();
    String [] containers = parseInstanceDistributionContainers(instanceDistribution);
    for (String container : containers) {
      String[] tokens = container.split(":");
      assertTrue(tokens.length > 3 && (tokens.length - 1) % 3 == 0,
          "Invalid instance distribution format. Expected componentId "
              + "followed by instance triples: %s", instanceDistribution);
      Set<String> idsFound = new HashSet<>();
      for (int i = 1; i < tokens.length; i += 3) {
        String instanceName = tokens[i];
        String instanceId = tokens[i + 1];
        assertTrue(!idsFound.contains(instanceId),
            "Duplicate instanceId (%s) found in instance distribution %s for instance %s %s",
            instanceId, instanceDistribution, instanceName, i);
        idsFound.add(instanceId);
        Integer occurrences = componentParallelism.getOrDefault(instanceName, 0);
        componentParallelism.put(instanceName, occurrences + 1);
      }
    }
    return componentParallelism;
  }

  private static String[] parseInstanceDistributionContainers(String instanceDistribution) {
    return instanceDistribution.split(",");
  }

  private static PackingPlans.PackingPlan createPackingPlan(String instanceDistribution,
                                                            String componentRamDistribution) {
    PackingPlans.PackingPlan.Builder builder = PackingPlans.PackingPlan.newBuilder();
    builder.setInstanceDistribution(instanceDistribution);
    builder.setComponentRamDistribution(componentRamDistribution);
    return builder.build();
  }

  private static TopologyAPI.Topology mergeTopology(TopologyAPI.Topology topology,
                                                    String componentName,
                                                    int parallelism) {
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
    for (int i = 0; i < builder.getBoltsCount(); i++) {
      TopologyAPI.Bolt.Builder boltBuilder = builder.getBoltsBuilder(i);
      TopologyAPI.Component.Builder compBuilder = boltBuilder.getCompBuilder();
      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry
          : compBuilder.getAllFields().entrySet()) {
        if (entry.getKey().getName().equals("name") && componentName.equals(entry.getValue())) {
          TopologyAPI.Config.Builder confBuilder = compBuilder.getConfigBuilder();
          boolean keyFound = false;
          for (TopologyAPI.Config.KeyValue.Builder kvBuilder : confBuilder.getKvsBuilderList()) {
            if (kvBuilder.getKey().equals(
                com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM)) {
              kvBuilder.setValue(Integer.toString(parallelism));
              keyFound = true;
              break;
            }
          }
          if (!keyFound) {
            TopologyAPI.Config.KeyValue.Builder kvBuilder =
                TopologyAPI.Config.KeyValue.newBuilder();
            kvBuilder.setKey(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM);
            kvBuilder.setValue(Integer.toString(parallelism));
            confBuilder.addKvs(kvBuilder);
          }
        }
      }
    }
    return builder.build();
  }

  private static boolean runProcess(List<String> auroraCmd) {
    boolean isVerbose = true;
    return 0 == ShellUtils.runProcess(
        isVerbose, auroraCmd.toArray(new String[0]), new StringBuilder(), new StringBuilder());
  }

  private static void assertTrue(boolean condition, String message, Object... values) {
    if (!condition) {
      throw new RuntimeException("ERROR: " + String.format(message, values));
    }
  }

  private static void print(String format, Object... values) {
    System.out.println(String.format(format, values));
  }
}
