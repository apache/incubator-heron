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
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;

/**
 * Utility to process TopologyAPI.Topology proto
 */
public final class TopologyUtils {
  private static final Logger LOG = Logger.getLogger(TopologyUtils.class.getName());

  private TopologyUtils() {
  }

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

  public static int getTotalInstance(TopologyAPI.Topology topology) {
    Map<String, Integer> parallelismMap = getComponentParallelism(topology);
    int numInstances = 0;
    for (int parallelism : parallelismMap.values()) {
      numInstances += parallelism;
    }
    return numInstances;
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

    // Only verify ram map string well-formed.
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
   * Parses the value in Config.TOPOLOGY_COMPONENT_RAMMAP,
   * and returns a map containing only component specified.
   * Returns a empty map if the Config is not set
   *
   * @param topology the topology def
   * @return a map (componentName -&gt; ram required)
   */
  public static Map<String, ByteAmount> getComponentRamMapConfig(TopologyAPI.Topology topology) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    Map<String, ByteAmount> ramMap = new HashMap<>();

    // Get the set of component names to make sure the config only specifies valid component name
    Set<String> componentNames = getComponentParallelism(topology).keySet();

    // Parse the config value
    String ramMapStr = getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_COMPONENT_RAMMAP, (String) null);
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
        if (!componentNames.contains(componentAndRam[0])) {
          throw new RuntimeException("Invalid component. " + componentAndRam[0] + " not found");
        }
        long requiredRam = Long.parseLong(componentAndRam[1]);

        ramMap.put(componentAndRam[0], ByteAmount.fromBytes(requiredRam));
      }
    }
    return ramMap;
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
}
