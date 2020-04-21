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

package org.apache.heron.apiserver.utils;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.scheduler.utils.SubmitterUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.ConfigLoader;
import org.apache.heron.spi.common.Key;

public final class ConfigUtils {

  private static final String CONFIG_SUFFIX = ".yaml";

  public static Config.Builder builder(Config baseConfiguration) {
    return Config.newBuilder().putAll(baseConfiguration);
  }

  public static String createOverrideConfiguration(Properties properties) throws IOException {
    final Path overridesPath = Files.createTempFile("overrides-", CONFIG_SUFFIX);
    try (Writer writer = Files.newBufferedWriter(overridesPath)) {
      final Map<Object, Object> overrides = new HashMap<>();
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        overrides.put(entry.getKey(), entry.getValue());
      }
      final Yaml yaml = newYaml();
      yaml.dump(overrides, writer);

      return overridesPath.toFile().getAbsolutePath();
    } finally {
      overridesPath.toFile().deleteOnExit();
    }
  }

  public static Config getBaseConfiguration(String heronDirectory,
        String heronConfigDirectory,
        String releaseFile,
        String overrideConfigurationFile) {
    // TODO add release file
    Config config = ConfigLoader.loadConfig(heronDirectory,
        heronConfigDirectory,
        releaseFile,
        overrideConfigurationFile);
    // Put location of the override file in the config so that schedulers invoked by
    // the API server can load the override configs if needed. OVERRIDE_YAML cannot be used
    // to set this because then the location will get passed on to the heron executors
    return Config.newBuilder()
        .putAll(config)
        .put(Key.APISERVER_OVERRIDE_YAML, overrideConfigurationFile)
        .build();
  }

  public static Config getTopologyConfig(String topologyPackage, String topologyBinaryFile,
        String topologyDefinitionFile) {

    final TopologyAPI.Topology topology;
    try {
      topology = TopologyUtils.getTopology(topologyDefinitionFile);
    } catch (InvalidTopologyException e) {
      throw new RuntimeException(e);
    }
    return SubmitterUtils.topologyConfigs(
        topologyPackage,
        topologyBinaryFile,
        topologyDefinitionFile,
        topology);
  }

  @SuppressWarnings("unchecked")
  public static void applyOverrides(Path overridesPath, Map<String, String> overrides)
      throws IOException {
    if (overrides.isEmpty()) {
      return;
    }
    final Path tempOverridesPath = Files.createTempFile("overrides-", CONFIG_SUFFIX);

    Reader overrideReader = null;
    try (Writer writer = Files.newBufferedWriter(tempOverridesPath)) {
      overrideReader = Files.newBufferedReader(overridesPath);
      final Map<String, Object> currentOverrides =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(overrideReader);
      currentOverrides.putAll(overrides);

      // write updated overrides
      newYaml().dump(currentOverrides, writer);

      // close override file so we can replace it with the updated overrides
      overrideReader.close();

      FileHelper.copy(tempOverridesPath, overridesPath);
    } finally {
      tempOverridesPath.toFile().delete();
      SysUtils.closeIgnoringExceptions(overrideReader);
    }
  }

  // this is needed because the heron executor ignores the override.yaml
  @SuppressWarnings("unchecked")
  public static void applyOverridesToStateManagerConfig(Path overridesPath,
        Path stateManagerPath) throws IOException {
    final Path tempStateManagerPath = Files.createTempFile("statemgr-", CONFIG_SUFFIX);
    Reader stateManagerReader = null;
    try (
        Reader overrideReader = Files.newBufferedReader(overridesPath);
        Writer writer = Files.newBufferedWriter(tempStateManagerPath);
    ) {
      stateManagerReader = Files.newBufferedReader(stateManagerPath);

      final Map<String, Object> overrides =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(overrideReader);
      final Map<String, Object> stateMangerConfig =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(stateManagerReader);
      // update the state manager config with the overrides
      for (Map.Entry<String, Object> entry : overrides.entrySet()) {
        // does this key have an override?
        if (stateMangerConfig.containsKey(entry.getKey())) {
          stateMangerConfig.put(entry.getKey(), entry.getValue());
        }
      }

      // write new state manager config
      newYaml().dump(stateMangerConfig, writer);

      // close state manager file so we can replace it with the updated configuration
      stateManagerReader.close();

      FileHelper.copy(tempStateManagerPath, stateManagerPath);
    } finally {
      tempStateManagerPath.toFile().delete();
      SysUtils.closeIgnoringExceptions(stateManagerReader);
    }
  }

  private static Yaml newYaml() {
    final DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);

    return new Yaml(options);
  }

  private ConfigUtils() {
  }
}
