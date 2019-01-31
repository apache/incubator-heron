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


package org.apache.heron.api.utils;

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
import java.util.logging.Logger;

import org.apache.heron.api.Config;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.ByteAmount;

/**
 * Utility to process TopologyAPI.Topology proto
 */
public final class TopologyUtils {
  private static final Logger LOG = Logger.getLogger(TopologyUtils.class.getName());

  private TopologyUtils() {
  }

  public static TopologyAPI.Topology getTopology(String topologyDefnFile)
      throws InvalidTopologyException {
    try {
      byte[] topologyDefn = Files.readAllBytes(Paths.get(topologyDefnFile));
      TopologyAPI.Topology topology = TopologyAPI.Topology.parseFrom(topologyDefn);
      validateTopology(topology);

      return topology;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read/parse content of " + topologyDefnFile, e);
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

  public static Long getConfigWithDefault(
      List<TopologyAPI.Config.KeyValue> config, String key, Long defaultValue) {
    return Long.parseLong(getConfigWithDefault(config, key, Long.toString(defaultValue)));
  }

  public static Integer getConfigWithDefault(
      List<TopologyAPI.Config.KeyValue> config, String key, Integer defaultValue) {
    return Integer.parseInt(getConfigWithDefault(config, key, Integer.toString(defaultValue)));
  }

  public static Double getConfigWithDefault(
      List<TopologyAPI.Config.KeyValue> config, String key, Double defaultValue) {
    return Double.parseDouble(getConfigWithDefault(config, key, Double.toString(defaultValue)));
  }

  public static ByteAmount getConfigWithDefault(
      List<TopologyAPI.Config.KeyValue> config, String key, ByteAmount defaultValue) {
    long defaultBytes = defaultValue.asBytes();
    return ByteAmount.fromBytes(getConfigWithDefault(config, key, defaultBytes));
  }

  public static Boolean getConfigWithDefault(
      List<TopologyAPI.Config.KeyValue> config, String key, boolean defaultValue) {
    return Boolean.parseBoolean(getConfigWithDefault(config, key, Boolean.toString(defaultValue)));
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
    return getConfigWithDefault(topologyConfig, Config.TOPOLOGY_WORKER_CHILDOPTS, "");
  }

  public static String getComponentJvmOptions(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return getConfigWithDefault(topologyConfig, Config.TOPOLOGY_COMPONENT_JVMOPTS, "");
  }

  public static int getTotalInstance(Map<String, Integer> parallelismMap) {
    int numInstances = 0;
    for (int parallelism : parallelismMap.values()) {
      numInstances += parallelism;
    }
    return numInstances;
  }

  public static int getTotalInstance(TopologyAPI.Topology topology) {
    Map<String, Integer> parallelismMap = getComponentParallelism(topology);
    return getTotalInstance(parallelismMap);
  }

  public static boolean shouldStartCkptMgr(TopologyAPI.Topology topology) {
    String mode = getConfigWithDefault(topology.getTopologyConfig().getKvsList(),
                                       Config.TOPOLOGY_RELIABILITY_MODE, "");
    if (mode.isEmpty()) {
      return false;
    } else {
      return Config.TopologyReliabilityMode.valueOf(mode)
             == Config.TopologyReliabilityMode.EFFECTIVELY_ONCE;
    }
  }

  public static ByteAmount getCheckpointManagerRam(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return TopologyUtils.getConfigWithDefault(topologyConfig,
        org.apache.heron.api.Config.TOPOLOGY_STATEFUL_CKPTMGR_RAM,
       ByteAmount.fromGigabytes(1));
  }

  /**
   * Throw a IllegalArgumentException if verifyTopology returns false
   * @param topology to validate
   */
  public static void validateTopology(TopologyAPI.Topology topology)
      throws InvalidTopologyException {
    if (!TopologyUtils.verifyTopology(topology)) {
      throw new InvalidTopologyException();
    }
  }

  /**
   * Verify if the given topology has all the necessary information
   */
  public static boolean verifyTopology(TopologyAPI.Topology topology) {
    if (!topology.hasName() || topology.getName().isEmpty()) {
      LOG.severe("Missing topology name");
      return false;
    }
    if (topology.getName().contains(".") || topology.getName().contains("/")) {
      LOG.severe("Invalid topology name. Topology name shouldn't have . or /");
      return false;
    }

    // Only verify RAM map string well-formed.
    getComponentRamMapConfig(topology);
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

    return true;
  }

  public static String getAdditionalClassPath(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return getConfigWithDefault(topologyConfig, Config.TOPOLOGY_ADDITIONAL_CLASSPATH, "");
  }

  /**
   * Parses the value in Config.TOPOLOGY_COMPONENT_CPUMAP,
   * and returns a map containing only component specified.
   * Returns a empty map if the Config is not set
   *
   * @param topology the topology def
   * @return a map (componentName -&gt; cpu required)
   */
  public static Map<String, Double> getComponentCpuMapConfig(TopologyAPI.Topology topology)
      throws RuntimeException {
    Map<String, String> configMap =
        getComponentConfigMap(topology, Config.TOPOLOGY_COMPONENT_CPUMAP);
    Map<String, Double> cpuMap = new HashMap<>();

    for (Map.Entry<String, String> entry : configMap.entrySet()) {
      Double requiredCpu = Double.parseDouble(entry.getValue());
      cpuMap.put(entry.getKey(), requiredCpu);
    }
    return cpuMap;
  }

  /**
   * Parses the value in Config.TOPOLOGY_COMPONENT_RAMMAP,
   * and returns a map containing only component specified.
   * Returns a empty map if the Config is not set
   *
   * @param topology the topology def
   * @return a map (componentName -&gt; RAM required)
   */
  public static Map<String, ByteAmount> getComponentRamMapConfig(TopologyAPI.Topology topology)
      throws RuntimeException {
    Map<String, String> configMap =
        getComponentConfigMap(topology, Config.TOPOLOGY_COMPONENT_RAMMAP);
    Map<String, ByteAmount> ramMap = new HashMap<>();

    for (Map.Entry<String, String> entry : configMap.entrySet()) {
      long requiredRam = Long.parseLong(entry.getValue());
      ramMap.put(entry.getKey(), ByteAmount.fromBytes(requiredRam));
    }
    return ramMap;
  }

  /**
   * Parses the value in Config.TOPOLOGY_COMPONENT_DISKMAP,
   * and returns a map containing only component specified.
   * Returns a empty map if the Config is not set
   *
   * @param topology the topology def
   * @return a map (componentName -&gt; disk required)
   */
  public static Map<String, ByteAmount> getComponentDiskMapConfig(TopologyAPI.Topology topology)
      throws RuntimeException {
    Map<String, String> configMap =
        getComponentConfigMap(topology, Config.TOPOLOGY_COMPONENT_DISKMAP);
    Map<String, ByteAmount> diskMap = new HashMap<>();

    for (Map.Entry<String, String> entry : configMap.entrySet()) {
      long requiredDisk = Long.parseLong(entry.getValue());
      diskMap.put(entry.getKey(), ByteAmount.fromBytes(requiredDisk));
    }
    return diskMap;
  }

  /**
   * This is a util function to parse cpumap, rammap and diskmap. A config example:
   * "spout1:1,spout2:1,bolt1:5". The function validates component name and throws exception
   * if any component name is wrong in the config.
   */
  protected static Map<String, String> getComponentConfigMap(TopologyAPI.Topology topology,
                                                             String key) throws RuntimeException {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    Map<String, String> configMap = new HashMap<>();

    // Get the set of component names to make sure the config only specifies valid component name
    Set<String> componentNames = getComponentParallelism(topology).keySet();

    // Parse the config value
    String mapStr = getConfigWithDefault(topologyConfig, key, (String) null);
    if (mapStr != null) {
      String[] mapTokens = mapStr.split(",");
      // Each token should be in this format: component:value
      for (String token : mapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] componentAndValue = token.split(":");
        if (componentAndValue.length != 2) {
          throw new RuntimeException("Malformed component config " + key);
        }
        if (!componentNames.contains(componentAndValue[0])) {
          throw new RuntimeException("Invalid component. " + componentAndValue[0] + " not found");
        }
        configMap.put(componentAndValue[0], componentAndValue[1]);
      }
    }
    return configMap;
  }

  // TODO: in a PR of it's own rename this to getNumStreamManagers to be correct
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

  public static boolean getTopologyRemoteDebuggingEnabled(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return Boolean.parseBoolean(TopologyUtils.getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_REMOTE_DEBUGGING_ENABLE, "false"));
  }
}
