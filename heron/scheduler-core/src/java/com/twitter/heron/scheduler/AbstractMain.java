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
package com.twitter.heron.scheduler;

import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.scheduler.dryrun.DryRunResponse;
import com.twitter.heron.scheduler.dryrun.UpdateDryRunResponse;
import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.scheduler.dryrun.SubmitDryRunResponse;
import com.twitter.heron.scheduler.dryrun.SubmitDryRunRender;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.LauncherException;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.uploader.UploaderException;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TopologyUtils;
/**
 * Description TODO...
 */
public abstract class AbstractMain {

  protected final Logger LOG = Logger.getLogger(this.getClass().getName());

   /**
   * Load the topology config
   *
   * @param topologyPackage, tar ball containing user submitted jar/tar, defn and config
   * @param topologyBinaryFile, name of the user submitted topology jar/tar/pex file
   * @param topology, proto in memory version of topology definition
   * @return config, the topology config
   */
  protected static Config topologyConfigs(
      String topologyPackage, String topologyBinaryFile, String topologyDefnFile,
      TopologyAPI.Topology topology) {
    PackageType packageType = PackageType.getPackageType(topologyBinaryFile);

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinitionFile(), topologyDefnFile)
        .put(Keys.topologyPackageFile(), topologyPackage)
        .put(Keys.topologyBinaryFile(), topologyBinaryFile)
        .put(Keys.topologyPackageType(), packageType)
        .build();
    return config;
  }

   /**
   * Load the defaults config
   *
   * @param heronHome, directory of heron home
   * @param configPath, directory containing the config
   * @param releaseFile, release file containing build information
   * <p>
   * return config, the defaults config
   */
  protected static Config defaultConfigs(String heronHome, String configPath, String releaseFile) {
    Config config = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, configPath, releaseFile))
        .build();
    return config;
  }

   /**
   * Load the override config from cli
   *
   * @param overrideConfigPath, override config file path
   * <p>
   * @return config, the override config
   */
  protected static Config overrideConfigs(String overrideConfigPath) {
    Config config = Config.newBuilder()
        .putAll(ClusterConfig.loadOverrideConfig(overrideConfigPath))
        .build();
    return config;
  }

  protected static Options constructCommonOptions() {
    Options options = new Options();

    Option cluster = Option.builder("c")
        .desc("Cluster name in which the topology needs to run on")
        .longOpt("cluster")
        .hasArgs()
        .argName("cluster")
        .required()
        .build();

    Option role = Option.builder("r")
        .desc("Role under which the topology needs to run")
        .longOpt("role")
        .hasArgs()
        .argName("role")
        .required()
        .build();

    Option environment = Option.builder("e")
        .desc("Environment under which the topology needs to run")
        .longOpt("environment")
        .hasArgs()
        .argName("environment")
        .required()
        .build();

    Option configFile = Option.builder("p")
        .desc("Path of the config files")
        .longOpt("config_path")
        .hasArgs()
        .argName("config path")
        .required()
        .build();

    Option configOverrides = Option.builder("o")
        .desc("Command line override config path")
        .longOpt("override_config_file")
        .hasArgs()
        .argName("override config file")
        .build();

    Option heronHome = Option.builder("d")
        .desc("Directory where heron is installed")
        .longOpt("heron_home")
        .hasArgs()
        .argName("heron home dir")
        .required()
        .build();

    Option releaseFile = Option.builder("b")
        .desc("Release file name")
        .longOpt("release_file")
        .hasArgs()
        .argName("release information")
        .build();

    Option dryRun = Option.builder("u")
        .desc("dry-run")
        .longOpt("dry_run")
        .required(false)
        .build();

    Option dryRunFormat = Option.builder("t")
        .desc("dry-run format")
        .longOpt("dry_run_format")
        .hasArg()
        .required(false)
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(configFile);
    options.addOption(configOverrides);
    options.addOption(releaseFile);
    options.addOption(heronHome);
    options.addOption(dryRun);
    options.addOption(dryRunFormat);
    options.addOption(verbose);

    return options;
  }

  // construct command line help options
  protected static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @param verbose, enable verbose logging
   * @return config, the command line config
   */
  protected static Config commandLineConfigs(String cluster,
                                             String role,
                                             String environ,
                                             Boolean dryRun,
                                             String dryRunFormat,
                                             Boolean verbose) {
    Config config = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.dryRun(), dryRun)
        .put(Keys.dryRunFormat(), dryRunFormat)
        .put(Keys.verbose(), verbose)
        .build();

    return config;
  }

  private void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(this.getClass().getSimpleName(), options);
  }

  private static final int EXIT_CODE_FAIL = 100;
  private static final int EXIT_CODE_DRYRUN = 200;

  protected abstract Options constructOptions();

  protected abstract CommandLineParser builderCommandLineParser();

  protected abstract Config buildConfig(CommandLine cmd);

  protected abstract void run(Config config);

  protected abstract String renderDryRunResponse(DryRunResponse response);

  protected void doMain(String[] args) throws Exception {
    Options options = constructOptions();
    Options helpOptions = constructHelpOptions();
    CommandLineParser parser = new DefaultParser();
    // parse the help options first.
    CommandLine cmd = parser.parse(helpOptions, args, true);

    if (cmd.hasOption("h")) {
      usage(options);
      return;
    }

    try {
      // Now parse the required options
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      usage(options);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    // Set logging level
    Level logLevel = Level.INFO;
    if (cmd.hasOption("v")) {
      logLevel = Level.ALL;
    }

    // init log
    LoggingHelper.loggerInit(logLevel, false);

    // build config
    Config config = buildConfig(cmd);

    // run
    /* Meaning of exit status code:
       - status code = 0:
         program exits without error
       - 0 < status code < 100:
         program fails to execute before program execution. For example,
         JVM cannot find or load main class
       - 100 <= status code < 200:
         program fails to launch after program execution. For example,
         topology definition file fails to be loaded
       - status code >= 200
         program sends out dry-run response */
    try {
      run(config);
    } catch (SubmitDryRunResponse response) {
      System.out.println(renderDryRunResponse(response));
      // Exit with status code 200 to indicate dry-run response is sent out
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(EXIT_CODE_DRYRUN);
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      /* Since only stderr is used (by logging), we use stdout here to
       propagate error message back to Python's executor.py (invoke site). */
      /* TODO: better word */
      LOG.log(Level.FINE, "Exception when executing main function", e);
      System.out.println(e.getMessage());
      // Exit with status code 100 to indicate that error has happened on user-land
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(EXIT_CODE_FAIL);
    }
  }
}
