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

import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;

/**
 * For loading scheduler config
 */
public final class SchedulerConfig {
  private static final Logger LOG = Logger.getLogger(SchedulerConfig.class.getName());

  private SchedulerConfig() {

  }

  /**
   * Load the topology config
   *
   * @param topologyJarFile, name of the user submitted topology jar/tar file
   * @param topologyDefnFile, name of the topology defintion file
   * @param topology, proto in memory version of topology definition
   * @return config, the topology config
   */
  protected static Config topologyConfigs(String topologyJarFile,
                                          String topologyDefnFile, TopologyAPI.Topology topology) {

    String pkgType = FileUtils.isOriginalPackageJar(
        FileUtils.getBaseName(topologyJarFile)) ? "jar" : "tar";

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinitionFile(), topologyDefnFile)
        .put(Keys.topologyJarFile(), topologyJarFile)
        .put(Keys.topologyPackageType(), pkgType)
        .build();

    return config;
  }

  /**
   * Load the defaults config
   * <p>
   * return config, the defaults config
   */
  protected static Config sandboxConfigs() {
    Config config = Config.newBuilder()
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(ClusterConfig.loadSandboxConfig())
        .build();
    return config;
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @return config, the command line config
   */
  protected static Config commandLineConfigs(String cluster, String role, String environ) {
    Config config = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .build();
    return config;
  }

  // build the config by expanding all the variables
  public static Config loadConfig(
      String cluster,
      String role,
      String environ,
      String topologyJarFile,
      String topologyDefnFile,
      TopologyAPI.Topology topology) {

    Config config = Config.expand(
        Config.newBuilder()
            .putAll(sandboxConfigs())
            .putAll(commandLineConfigs(cluster, role, environ))
            .putAll(topologyConfigs(topologyJarFile, topologyDefnFile, topology))
            .build());

    return config;
  }
}
