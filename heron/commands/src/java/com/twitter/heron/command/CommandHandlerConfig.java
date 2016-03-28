package com.twitter.heron.command;

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

import org.apache.commons.cli.CommandLine;

/**
 * For loading command handler config
 */
public class CommandHandlerConfig {
  private static final Logger LOG = Logger.getLogger(CommandHandlerConfig.class.getName());

  /**
   * Load the defaults config
   *
   * @return config, the defaults config
   */
  protected static Config defaultConfigs(String heronHome, String configPath) {
    Config config = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterConfig.loadCommandsConfig(heronHome, configPath))
        .build();
    return config;
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   *
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

  /**
   * Load the config from static config files
   *
   * @param commandLine, the command line args provided
   *
   * @return config, the static config
   */
  protected static Config loadConfig(CommandLine commandLine) {

    String cluster = commandLine.getOptionValue("cluster");
    String role = commandLine.getOptionValue("role");
    String environ = commandLine.getOptionValue("environment");
    String heronHome = commandLine.getOptionValue("heron_home");
    String configPath = commandLine.getOptionValue("config_path");

    // build the config by expanding all the variables
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultConfigs(heronHome, configPath))
            .putAll(commandLineConfigs(cluster, role, environ))
            .build());
    return config;
  }
}
