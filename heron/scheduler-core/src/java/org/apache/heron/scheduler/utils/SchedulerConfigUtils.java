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

package org.apache.heron.scheduler.utils;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.PackageType;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.ConfigLoader;
import org.apache.heron.spi.common.Key;

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
        .put(Key.TOPOLOGY_ID, topology.getId())
        .put(Key.TOPOLOGY_NAME, topology.getName())
        .put(Key.TOPOLOGY_DEFINITION_FILE, topologyDefnFile)
        .put(Key.TOPOLOGY_BINARY_FILE, topologyBinaryFile)
        .put(Key.TOPOLOGY_PACKAGE_TYPE, packageType)
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
        .put(Key.CLUSTER, cluster)
        .put(Key.ROLE, role)
        .put(Key.ENVIRON, environ)
        .put(Key.VERBOSE, verbose)
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

    return Config.toClusterMode(
        Config.newBuilder()
            .putAll(ConfigLoader.loadClusterConfig())
            .putAll(commandLineConfigs(cluster, role, environ, verbose))
            .putAll(topologyConfigs(topologyBinaryFile, topologyDefnFile, topology))
            .build());
  }
}
