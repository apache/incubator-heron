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

package com.twitter.heron.statemgr.localfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.WatchCallback;
import com.twitter.heron.statemgr.FileSystemStateManager;

public class LocalFileSystemStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemStateManager.class.getName());

  @Override
  public void initialize(Config ipconfig) {

    super.initialize(ipconfig);

    // By default, we would init the file tree if it is not there
    boolean isInitLocalFileTree = LocalFileSystemContext.initLocalFileTree(ipconfig);

    if (isInitLocalFileTree && !initTree()) {
      throw new IllegalArgumentException("Failed to initialize Local State manager. "
          + "Check rootAddress: " + rootAddress);
    }
  }

  protected boolean initTree() {
    List<Pair<String, String>> dirNamesAndPaths = new ArrayList<>();
    dirNamesAndPaths.add(Pair.create("Topologies", getTopologyDir()));
    dirNamesAndPaths.add(Pair.create("Tmaster location", getTMasterLocationDir()));
    dirNamesAndPaths.add(Pair.create("Packing plan", getPackingPlanDir()));
    dirNamesAndPaths.add(Pair.create("Physical plan", getPhysicalPlanDir()));
    dirNamesAndPaths.add(Pair.create("Execution state", getExecutionStateDir()));
    dirNamesAndPaths.add(Pair.create("Scheduler location", getSchedulerLocationDir()));

    // Make necessary directories
    for (Pair<String, String> dirNamesAndPath : dirNamesAndPaths) {
      LOG.log(Level.FINE,
          String.format("%s directory: %s", dirNamesAndPath.first, dirNamesAndPath.second));
      if(!FileUtils.isDirectoryExists(dirNamesAndPath.second) &&
         !FileUtils.createDirectory(dirNamesAndPath.second)) {
        return false;
      }
    }

    return true;
  }

  // Make utils class protected for easy unit testing
  protected ListenableFuture<Boolean> setData(String path, byte[] data, boolean overwrite) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(path, data, overwrite);
    future.set(ret);

    return future;
  }

  @Override
  protected ListenableFuture<Boolean> nodeExists(String path) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.isFileExists(path);
    future.set(ret);

    return future;
  }

  @Override
  protected ListenableFuture<Boolean> deleteNode(String path) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.deleteFile(path);
    future.set(ret);

    return future;
  }

  @Override
  @SuppressWarnings("unchecked") // we don't know what M is until runtime
  protected <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                                String path,
                                                                Message.Builder builder) {
    final SettableFuture<M> future = SettableFuture.create();
    byte[] data = FileUtils.readFromFile(path);
    if (data.length == 0) {
      future.set(null);
      return future;
    }

    try {
      builder.mergeFrom(data);
      future.set((M) builder.build());
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse " + Message.Builder.class, e));
    }

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    return setData(getExecutionStatePath(topologyName), executionState.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setTMasterLocation(
      TopologyMaster.TMasterLocation location, String topologyName) {
    // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
    // This is because when running in simulator we control when a tmaster dies and
    // comes up deterministically.
    return setData(getTMasterLocationPath(topologyName), location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    return setData(getTopologyPath(topologyName), topology.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    return setData(getPhysicalPlanPath(topologyName), physicalPlan.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setPackingPlan(
      PackingPlans.PackingPlan packingPlan, String topologyName) {
    return setData(getPackingPlanPath(topologyName), packingPlan.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location, String topologyName) {
    // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
    // This is because when running in simulator we control when a scheduler dies and
    // comes up deterministically.
    return setData(getSchedulerLocationPath(topologyName), location.toByteArray(), true);
  }

  @Override
  public void close() {
    // We would not clear anything here
    // Scheduler kill interface should take care of the cleaning
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
≈   */
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    if (args.length < 1) {
      throw new RuntimeException(String.format(
          "Usage: java %s <topology_name> - view state manager details for a topology",
          LocalFileSystemStateManager.class.getCanonicalName()));
    }

    String topologyName = args[0];
    Config config = Config.newBuilder()
        .put(Keys.stateManagerRootPath(),
            System.getProperty("user.home") + "/.herondata/repository/state/local")
        .build();

    print("==> State Manager root path: %s", config.get(Keys.stateManagerRootPath()));

    com.twitter.heron.spi.statemgr.IStateManager stateManager = new LocalFileSystemStateManager();
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
    } else {
      print("==> Topology %s not found under %s",
          topologyName, config.get(Keys.stateManagerRootPath()));
    }

    if (args.length == 2) {
      String proposedInstanceDistribution = args[1];

      Map<String, Integer> proposedChanges = parallelismChanges(
          existingPackingPlan.getInstanceDistribution(),
          proposedInstanceDistribution);

      PackingPlans.PackingPlan newPackingPlan = createPackingPlan(
          proposedInstanceDistribution,
          existingPackingPlan.getComponentRamDistribution());

      TopologyAPI.Topology updatedTopology = stateManager.getTopology(null, topologyName).get();
      for (Map.Entry<String, Integer> proposedChange : proposedChanges.entrySet()) {
        String componentName = proposedChange.getKey();
        Integer parallelism = proposedChange.getValue();
        print("Updating packing plan to change component %s to parallelism=%s", componentName,
            parallelism);

        updatedTopology = updateTopology(updatedTopology, componentName, parallelism);
      }

      //update parallelism in topology since TMaster checks that Sum(parallelism) == Sum(instances)
      boolean success = stateManager.setTopology(updatedTopology, topologyName).get();
      print("==> Set Topology: %s", success);

      updatedTopology = stateManager.getTopology(null, topologyName).get();
      print("==> Got Topology: %s", updatedTopology);

      success = stateManager.deletePhysicalPlan(topologyName).get();
      print("==> Deleted Physical Plan: %s", success);

      stateManager.setPackingPlan(newPackingPlan, topologyName);
      existingPackingPlan = stateManager.getPackingPlan(null, topologyName).get();
      print("==> Updated PackingPlan:\n%s", existingPackingPlan);
    }
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
        "key %s in proposed instance distribution %s not found in current instance distribution %s",
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

  // given a string like "1:word:3:0:exclaim1:2:0:exclaim1:1:0" returns a map of
  // { word -> 1, explain1 -> 2 }
  private static Map<String, Integer> parseInstanceDistribution(String instanceDistribution) {
    Map<String, Integer> componentParallelism = new HashMap<>();
    String[] tokens = instanceDistribution.split(":");
    assertTrue(tokens.length > 3 && (tokens.length - 1) % 3 == 0,
        "Invalid instance distribution format. Expected componentId " +
        "followed by instance triples: %s", instanceDistribution);
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
    return componentParallelism;
  }

  private static PackingPlans.PackingPlan createPackingPlan(String instanceDistribution,
                                                            String componentRamDistribution) {
    PackingPlans.PackingPlan.Builder builder = PackingPlans.PackingPlan.newBuilder();
    builder.setInstanceDistribution(instanceDistribution);
    builder.setComponentRamDistribution(componentRamDistribution);
    return builder.build();
  }

  private static TopologyAPI.Topology updateTopology(TopologyAPI.Topology topology,
                                                     String componentName,
                                                     int parallelism) {
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
    for (int i = 0; i < builder.getBoltsCount(); i++) {
      TopologyAPI.Bolt.Builder boltBuilder = builder.getBoltsBuilder(i);
      TopologyAPI.Component.Builder compBuilder = boltBuilder.getCompBuilder();
      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : compBuilder.getAllFields().entrySet()) {
        if (entry.getKey().getName().equals("name") && componentName.equals(entry.getValue())) {
          TopologyAPI.Config.Builder confBuilder = compBuilder.getConfigBuilder();
          boolean keyFound = false;
          for (TopologyAPI.Config.KeyValue.Builder kvBuilder : confBuilder.getKvsBuilderList()) {
            if (kvBuilder.getKey().equals(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM)) {
              kvBuilder.setValue(Integer.toString(parallelism));
              keyFound = true;
              break;
            }
          }
          if (!keyFound) {
            TopologyAPI.Config.KeyValue.Builder kvBuilder = TopologyAPI.Config.KeyValue.newBuilder();
            kvBuilder.setKey(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM);
            kvBuilder.setValue(Integer.toString(parallelism));
            confBuilder.addKvs(kvBuilder);
          }
        }
      }
    }
    return builder.build();
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
