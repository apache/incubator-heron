package com.twitter.heron.spi.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

/**
 * Utility to process TopologyAPI.Topology proto
 */
public class TopologyUtils {
  private static final Logger LOG = Logger.getLogger(TopologyUtils.class.getName());

  public static TopologyAPI.Topology getTopology(String topologyDefnFile) {
    try {
      byte[] topologyDefn = Files.readAllBytes(Paths.get(topologyDefnFile));
      TopologyAPI.Topology topology = TopologyAPI.Topology.parseFrom(topologyDefn);
      if (!TopologyUtils.verifyTopology(topology)) {
        throw new RuntimeException("Topology object is Malformed");
      }

      return topology;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read/parse content of " + topologyDefnFile);
    }
  }

  public static String getConfigWithDefault(
      List<TopologyAPI.Config.KeyValue> config, String key, String defaultValue) {
    for (TopologyAPI.Config.KeyValue kv : config) {
      if (kv.getKey().equals(key)) {
        return kv.getValue();
      }
    }
    return defaultValue;
  }

  public static String getConfigWithException(
      List<TopologyAPI.Config.KeyValue> config, String key) {
    for (TopologyAPI.Config.KeyValue kv : config) {
      if (kv.getKey().equals(key)) {
        return kv.getValue();
      }
    }
    throw new RuntimeException("Missing config for required key " + key);
  }

  public static Map<String, Integer> getComponentParallelism(TopologyAPI.Topology topology) {
    Map<String, Integer> parallelismMap = new HashMap<>();
    for (TopologyAPI.Spout spout : topology.getSpoutsList()) {
      String componentName = spout.getComp().getName();
      String parallelism = getConfigWithException(
          spout.getComp().getConfig().getKvsList(), Config.TOPOLOGY_COMPONENT_PARALLELISM).trim();
      parallelismMap.put(componentName, Integer.parseInt(parallelism));
    }
    for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
      String componentName = bolt.getComp().getName();
      String parallelism = getConfigWithException(
          bolt.getComp().getConfig().getKvsList(), Config.TOPOLOGY_COMPONENT_PARALLELISM).trim();
      parallelismMap.put(componentName, Integer.parseInt(parallelism));
    }
    return parallelismMap;
  }

  public static String getInstanceJvmOptions(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return
        getConfigWithDefault(topologyConfig, Config.TOPOLOGY_WORKER_CHILDOPTS, "");
  }

  public static String getComponentJvmOptions(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return
        getConfigWithDefault(topologyConfig, Config.TOPOLOGY_COMPONENT_JVMOPTS, "");
  }

  public static int getTotalInstance(TopologyAPI.Topology topology) {
    Map<String, Integer> parallelismMap = getComponentParallelism(topology);
    int numInstances = 0;
    for (int parallelism : parallelismMap.values()) {
      numInstances += parallelism;
    }
    return numInstances;
  }

  public static boolean verifyTopology(TopologyAPI.Topology topology) {
    if (!topology.hasName() || topology.getName().isEmpty()) {
      LOG.severe("Missing topology name");
      return false;
    }
    if (topology.getName().contains(".") || topology.getName().contains("/")) {
      LOG.severe("Invalid topology name. Topology name shouldn't have . or /");
      return false;
    }
    try {
      // Only verify ram map string well-formed.
      getComponentRamMap(topology, -1);
      // Verify all bolts input streams exist. First get all output streams.
      Set<String> outputStreams = new HashSet<>();
      for (TopologyAPI.Spout spout : topology.getSpoutsList()) {
        for (TopologyAPI.OutputStream stream : spout.getOutputsList()) {
          outputStreams.add(
              stream.getStream().getComponentName() + "/" + stream.getStream().getId());
        }
      }
      for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
        for (TopologyAPI.OutputStream stream : bolt.getOutputsList()) {
          outputStreams.add(
              stream.getStream().getComponentName() + "/" + stream.getStream().getId());
        }
      }
      // Match output streams with input streams.
      for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
        for (TopologyAPI.InputStream stream : bolt.getInputsList()) {
          String key = stream.getStream().getComponentName() + "/" + stream.getStream().getId();
          if (!outputStreams.contains(key)) {
            LOG.severe("Invalid input stream " + key + " existing streams are " + outputStreams);
            return false;
          }
        }
      }
      // TODO(nbhagat): Should we enforce all output stream must be consumed?
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Failed to identify resource requirements from topology", e);
      return false;
    }
    return true;
  }

  public static String getAdditionalClassPath(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return getConfigWithDefault(topologyConfig, Config.TOPOLOGY_ADDITIONAL_CLASSPATH, "");
  }


  public static Map<String, Long> getComponentRamMap(
      TopologyAPI.Topology topology, long defaultRam) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    Map<String, Long> ramMap = new HashMap<>();
    Set<String> componentNames = getComponentParallelism(topology).keySet();
    for (String componentName : componentNames) {
      ramMap.put(componentName, defaultRam);
    }

    // Apply Ram overrides
    String ramMapStr = getConfigWithDefault(topologyConfig, Config.TOPOLOGY_COMPONENT_RAMMAP, null);
    if (ramMapStr != null) {
      String[] ramMapTokens = ramMapStr.split(",");
      for (String token : ramMapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] componentAndRam = token.split(":");
        if (componentAndRam.length != 2) {
          throw new RuntimeException("Malformed component rammap");
        }
        if (!ramMap.containsKey(componentAndRam[0])) {
          throw new RuntimeException("Invalid component. " + componentAndRam[0] + " not found");
        }
        long requiredRam = Long.parseLong(componentAndRam[1]);
        if (requiredRam < 128L * Constants.MB && requiredRam > 0) {
          throw new RuntimeException(String.format(
              "Component %s require at least 128MB ram. Given on %d MB",
              componentAndRam[0], requiredRam / Constants.MB));
        }
        ramMap.put(componentAndRam[0], requiredRam);
      }
    }
    return ramMap;
  }

  // TODO(nbhagat): Formalize how to pass distribution to heron-executor.
  public static String packingToString(PackingPlan packing) {
    StringBuilder[] containerBuilder = new StringBuilder[packing.containers.size()];
    for (PackingPlan.ContainerPlan container : packing.containers.values()) {
      int index = Integer.parseInt(container.id);
      containerBuilder[index - 1] = new StringBuilder();

      for (PackingPlan.InstancePlan instance : container.instances.values()) {
        String[] tokens = instance.id.split(":");
        containerBuilder[index - 1].append(
            String.format("%s:%s:%s:", tokens[1], tokens[2], tokens[3]));
      }
      containerBuilder[index - 1].deleteCharAt(containerBuilder[index - 1].length() - 1);
    }

    StringBuilder packingBuilder = new StringBuilder();
    for (int i = 0; i < containerBuilder.length; ++i) {
      StringBuilder builder = containerBuilder[i];
      packingBuilder.append(String.format("%d:%s,", i + 1, builder.toString()));
    }
    packingBuilder.deleteCharAt(packingBuilder.length() - 1);

    return packingBuilder.toString();
  }

  public static String formatRamMap(Map<String, Long> ramMap) {
    StringBuilder ramMapBuilder = new StringBuilder();
    for (String component : ramMap.keySet()) {
      ramMapBuilder.append(String.format("%s:%d,", component, ramMap.get(component)));
    }
    ramMapBuilder.deleteCharAt(ramMapBuilder.length() - 1);
    return ramMapBuilder.toString();
  }

  public static int getNumContainers(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return Integer.parseInt(TopologyUtils.getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_STMGRS, "1").trim());
  }

  // TODO(nbhagat): libs is dependent on pants for building. Instead take classpath as argument.
  public static String makeClassPath(TopologyAPI.Topology topology, String originalPackageFile) {
    String originalPackage = new File(originalPackageFile).getName();
    StringBuilder classPathBuilder = new StringBuilder();
    // TODO(nbhagat): Take type of package as argument.
    if (originalPackage.endsWith(".jar")) {
      // Bundled jar
      classPathBuilder.append(originalPackage);
    } else {
      // Bundled tar
      String topologyJar = originalPackage.replace(".tar.gz", "").replace(".tar", "") + ".jar";
      classPathBuilder.append(String.format("libs/*:%s", topologyJar));
    }
    String additionalClasspath = TopologyUtils.getAdditionalClassPath(topology);
    if (!additionalClasspath.isEmpty()) {
      classPathBuilder.append(":");
      classPathBuilder.append(TopologyUtils.getAdditionalClassPath(topology));
    }
    return classPathBuilder.toString();
  }

  public static String lookUpTopologyDefnFile(String dir, String filename) {
    String pattern = String.format("glob:%s/%s.defn", dir, filename);

    PathMatcher matcher =
        FileSystems.getDefault().getPathMatcher(pattern);

    for (File file : new File(dir).listFiles()) {
      if (matcher.matches(file.toPath())) {
        // We would return the first one matched
        return file.getPath();
      }
    }

    throw new IllegalStateException("Failed to find topology defn file");
  }

  /**
   * Trim the topology definition for storing into state manager.
   * This is because the user generated spouts and bolts
   * might be huge.
   *
   * @return trimmed topology
   */
  public static TopologyAPI.Topology trimTopology(TopologyAPI.Topology topology) {

    // create a copy of the topology physical plan
    TopologyAPI.Topology.Builder builder = topology.newBuilder().mergeFrom(topology);

    // clear the state of user spout java objects - which can be potentially huge
    for (TopologyAPI.Spout.Builder spout : builder.getSpoutsBuilderList()) {
      spout.getCompBuilder().clearJavaObject();
    }

    // clear the state of user spout java objects - which can be potentially huge
    for (TopologyAPI.Bolt.Builder bolt : builder.getBoltsBuilderList()) {
      bolt.getCompBuilder().clearJavaObject();
    }

    return builder.build();
  }

  /**
   * Acquire the topology lock by trying to store the trimmed topology definition into the state manager
   * Make sure this method is invoked before doing any operations on submitting a topology.
   * No more submissions are allowed after topology lock has been acquired.
   * This lock is to guarantee no two topologies with the same name can run at the same time.
   *
   * @param statemgr the state manager to persist the lock
   * @param topology the info stored in the lock
   * <p/>
   * * @return true if acquired lock successfully
   */
  public static boolean acquireTopologyLock(SchedulerStateManagerAdaptor statemgr, TopologyAPI.Topology topology) {
    ListenableFuture<Boolean> booleanFuture;
    Boolean futureResult;

    booleanFuture =
        statemgr.setTopology(TopologyUtils.trimTopology(topology), topology.getName());
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      LOG.severe("Failed to set topology state.");
      // Check whether the topology has already been running
      Boolean isTopologyRunning =
          NetworkUtils.awaitResult(statemgr.isTopologyRunning(topology.getName()), 5, TimeUnit.SECONDS);

      if (isTopologyRunning != null && isTopologyRunning.equals(Boolean.TRUE)) {
        LOG.severe("Topology already exists");
      }

      return false;
    }

    return true;
  }

  /**
   * Release the topology lock by trying to remove the trimmed topology definition from the state manager
   * Make sure this method is invoked after doing any operations on killing a topology.
   * People can do the submission again once the lock is released.
   *
   * @param statemgr the state manager to persist the lock
   * @param topologyName the topologyName to remove lock
   * <p/>
   * * @return true if released lock successfully
   */
  public static boolean releaseTopologyLock(SchedulerStateManagerAdaptor statemgr, String topologyName) {
    ListenableFuture<Boolean> booleanFuture;
    Boolean futureResult;

    booleanFuture = statemgr.deleteTopology(topologyName);
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      LOG.severe("Failed to clear topology state");
      return false;
    }

    return true;
  }
}
