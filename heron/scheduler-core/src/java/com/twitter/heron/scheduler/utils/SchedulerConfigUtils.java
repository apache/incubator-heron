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

package com.twitter.heron.scheduler.utils;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;

/**
 * For loading scheduler config
 */
public final class SchedulerConfigUtils {

  private SchedulerConfigUtils() {

  }

  /**
   * Load the topology config
   *
   * @param topologyBinaryFile, name of the user submitted topology jar/tar/pex file
   * @param topologyDefnFile, name of the topology defintion file
   * @param topology, proto in memory version of topology definition
   * @return config, the topology config
   */
  private static Config topologyConfigs(String topologyBinaryFile,
                                          String topologyDefnFile, TopologyAPI.Topology topology) {
    PackageType packageType = PackageType.getPackageType(topologyBinaryFile);

    return Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinitionFile(), topologyDefnFile)
        .put(Keys.topologyBinaryFile(), topologyBinaryFile)
        .put(Keys.topologyPackageType(), packageType)
        .build();
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @return config, the command line config
   */
  private static Config commandLineConfigs(String cluster, String role,
                                             String environ, Boolean verbose) {
    return Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.verbose(), verbose)
        .build();
  }

  // build the config by expanding all the variables
  public static Config loadConfig(
      String cluster,
      String role,
      String environ,
      String topologyBinaryFile,
      String topologyDefnFile,
      Boolean verbose,
      TopologyAPI.Topology topology) {

    return Config.expand(
        Config.newBuilder()
            .putAll(ClusterConfig.loadSandboxConfig())
            .putAll(commandLineConfigs(cluster, role, environ, verbose))
            .putAll(topologyConfigs(topologyBinaryFile, topologyDefnFile, topology))
            .build());
  }
}
