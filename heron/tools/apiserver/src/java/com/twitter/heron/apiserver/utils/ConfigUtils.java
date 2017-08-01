//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.apiserver.utils;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.utils.SubmitterUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigLoader;

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
      final Yaml yaml = new Yaml();
      yaml.dump(overrides, writer);

      return overridesPath.toFile().getAbsolutePath();
    } finally {
      overridesPath.toFile().deleteOnExit();
    }
  }

  public static Config getBaseConfiguration(String heronDirectory,
        String heronConfigDirectory,
        String overrideConfigurationFile) {
    // TODO add release file
    return ConfigLoader.loadConfig(heronDirectory,
        heronConfigDirectory,
        "",
        overrideConfigurationFile);
  }

  public static Config getTopologyConfig(String topologyPackage, String topologyBinaryFile,
        String topologyDefinitionFile, TopologyAPI.Topology topology) {
    return SubmitterUtils.topologyConfigs(
        topologyPackage,
        topologyBinaryFile,
        topologyDefinitionFile,
        topology);
  }

  // this is needed because the heron executor ignores the override.yaml
  @SuppressWarnings("unchecked")
  public static void applyOverridesToStateManagerConfig(Path overridesPath,
        Path stateManagerPath) throws IOException {
    final Path tempStateManagerPath = Files.createTempFile("statemgr-", CONFIG_SUFFIX);
    try (
        Reader overrideReader = Files.newBufferedReader(overridesPath);
        Reader stateManagerReader = Files.newBufferedReader(stateManagerPath);
        Writer writer = Files.newBufferedWriter(tempStateManagerPath);
    ) {
      final Map<String, Object> overrides = (Map<String, Object>) new Yaml().load(overrideReader);
      final Map<String, Object> stateMangerConfig =
          (Map<String, Object>) new Yaml().load(stateManagerReader);
      // update the state manager config with the overrides
      for (Map.Entry<String, Object> entry : overrides.entrySet()) {
        // does this key have an override?
        if (stateMangerConfig.containsKey(entry.getKey())) {
          stateMangerConfig.put(entry.getKey(), entry.getValue());
        }
      }

      // write new state manager config
      new Yaml().dump(stateMangerConfig, writer);

      FileHelper.copy(tempStateManagerPath, stateManagerPath);
    } finally {
      tempStateManagerPath.toFile().delete();
    }
  }

  private ConfigUtils() {
  }
}
