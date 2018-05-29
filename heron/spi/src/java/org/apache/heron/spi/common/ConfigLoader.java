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

package org.apache.heron.spi.common;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.common.config.ConfigReader;

public final class ConfigLoader {

  private ConfigLoader() {
  }

  private static Config loadDefaults(String heronHome, String configPath) {
    return Config.newBuilder(true)
        .put(Key.HERON_HOME, heronHome)
        .put(Key.HERON_CONF, configPath)
        .build();
  }

  @VisibleForTesting
  static Config loadConfig(String file) {
    Map<String, Object> readConfig = ConfigReader.loadFile(file);
    return addFromFile(readConfig);
  }

  @VisibleForTesting
  static Config addFromFile(Map<String, Object> readConfig) {
    return Config.newBuilder().putAll(readConfig).build();
  }

  /**
   * Loads raw configurations from files under the heronHome and configPath. The returned config
   * must be converted to either local or cluster mode to trigger pattern substitution of wildcards
   * tokens.
   */
  public static Config loadConfig(String heronHome, String configPath,
                                  String releaseFile, String overrideConfigFile) {
    Config defaultConfig = loadDefaults(heronHome, configPath);
    Config localConfig = Config.toLocalMode(defaultConfig); //to token-substitute the conf paths

    Config.Builder cb = Config.newBuilder()
        .putAll(defaultConfig)
        .putAll(loadConfig(Context.clusterFile(localConfig)))
        .putAll(loadConfig(Context.clientFile(localConfig)))
        .putAll(loadConfig(Context.healthmgrFile(localConfig)))
        .putAll(loadConfig(Context.packingFile(localConfig)))
        .putAll(loadConfig(Context.schedulerFile(localConfig)))
        .putAll(loadConfig(Context.stateManagerFile(localConfig)))
        .putAll(loadConfig(Context.uploaderFile(localConfig)))
        .putAll(loadConfig(Context.downloaderFile(localConfig)))
        .putAll(loadConfig(Context.statefulConfigFile(localConfig)))
        .putAll(loadConfig(releaseFile))
        .putAll(loadConfig(overrideConfigFile));
    return cb.build();
  }

  /**
   * Loads raw configurations using the default configured heronHome and configPath on the cluster.
   * The returned config must be converted to either local or cluster mode to trigger pattern
   * substitution of wildcards tokens.
   */
  public static Config loadClusterConfig() {
    Config defaultConfig = loadDefaults(
        Key.HERON_CLUSTER_HOME.getDefaultString(), Key.HERON_CLUSTER_CONF.getDefaultString());
    Config clusterConfig = Config.toClusterMode(defaultConfig); //to token-substitute the conf paths

    Config.Builder cb = Config.newBuilder()
        .putAll(defaultConfig)
        .putAll(loadConfig(Context.packingFile(clusterConfig)))
        .putAll(loadConfig(Context.healthmgrFile(clusterConfig)))
        .putAll(loadConfig(Context.schedulerFile(clusterConfig)))
        .putAll(loadConfig(Context.stateManagerFile(clusterConfig)))
        .putAll(loadConfig(Context.uploaderFile(clusterConfig)))
        .putAll(loadConfig(Context.downloaderFile(clusterConfig)))
        .putAll(loadConfig(Context.statefulConfigFile(clusterConfig)));

    // Add the override config at the end to replace any existing configs
    cb.putAll(loadConfig(Context.overrideFile(clusterConfig)));

    return cb.build();
  }
}
