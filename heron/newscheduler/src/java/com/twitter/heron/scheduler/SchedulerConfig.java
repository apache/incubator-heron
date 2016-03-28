package com.twitter.heron.scheduler;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;

import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;

/**
 * For loading scheduler config
 */
public class SchedulerConfig {
  private static final Logger LOG = Logger.getLogger(SchedulerConfig.class.getName());

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
   * <p/>
   * return config, the defaults config
   */
  protected static Config defaultConfigs() {
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
  protected static Config loadConfig(String cluster, String role, String environ,
      String topologyJarFile, String topologyDefnFile, TopologyAPI.Topology topology) {

    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultConfigs())
            .putAll(commandLineConfigs(cluster, role, environ))
            .putAll(topologyConfigs(topologyJarFile, topologyDefnFile, topology))
            .build());

    return config;
  }
}
