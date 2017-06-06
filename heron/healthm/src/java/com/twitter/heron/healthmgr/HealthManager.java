// Copyright 2016 Microsoft. All rights reserved.
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

package com.twitter.heron.healthmgr;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.microsoft.dhalion.api.IHealthPolicy;
import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.core.EventManager;
import com.microsoft.dhalion.core.PoliciesExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.sensors.TrackerMetricsProvider;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigLoader;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;

/**
 * e.g. options
 * -d ~/.heron -p ~/.heron/conf/local -c local -e default -r userName -t AckingTopology
 */
public class HealthManager {
  private static final Logger LOG = Logger.getLogger(HealthManager.class.getName());
  private final Config config;
  private final String trackerURL;

  private Config runtime;
  private Injector injector;
  private SchedulerStateManagerAdaptor stateMgrAdaptor;
  private ISchedulerClient schedulerClient;

  private List<IHealthPolicy> healthPolicies = new ArrayList<>();
  private HealthPolicyConfigReader policyConfigReader;

  public HealthManager(Config config, String trackerURL) {
    this.config = config;
    this.trackerURL = trackerURL;
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    Options slaManagerCliOptions = constructCliOptions();

    // parse the help options first.
    Options helpOptions = constructHelpOptions();
    CommandLine cmd = parser.parse(helpOptions, args, true);
    if (cmd.hasOption("h")) {
      usage(slaManagerCliOptions);
      return;
    }

    try {
      cmd = parser.parse(slaManagerCliOptions, args);
    } catch (ParseException e) {
      usage(slaManagerCliOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    String heronHome = cmd.getOptionValue("heron_home");
    String configPath = cmd.getOptionValue("config_path");
    String overrideConfigFile = cmd.getOptionValue("override_config_file");
    String releaseFile = cmd.getOptionValue("release_file");
    String topologyName = cmd.getOptionValue("topology_name");
    String trackerURL = cmd.getOptionValue("trackerURL", "http://localhost:8888");
    Boolean verbose = cmd.hasOption("verbose");

    // build the final config by expanding all the variables
    Config config = Config.toLocalMode(Config.newBuilder()
        .putAll(ConfigLoader.loadConfig(heronHome, configPath, releaseFile, overrideConfigFile))
        .putAll(commandLineConfigs(cluster, role, environ, topologyName, verbose))
        .build());

    LOG.info("Static Heron config loaded successfully ");
    LOG.fine(config.toString());

    HealthManager healthManager = new HealthManager(config, trackerURL);

    LOG.info("Initializing health manager");
    healthManager.initialize();

    LOG.info("Starting Health Manager");

    PoliciesExecutor policyExecutor = new PoliciesExecutor(healthManager.healthPolicies);
    ScheduledFuture<?> future = policyExecutor.start();
    try {
      future.get();
    } finally {
      policyExecutor.destroy();
    }
  }

  public void initialize() throws ReflectiveOperationException, FileNotFoundException {
    this.stateMgrAdaptor = createStateMgrAdaptor();

    this.runtime = Config.newBuilder()
        .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, stateMgrAdaptor)
        .put(Key.TOPOLOGY_NAME, Context.topologyName(config))
        .build();

    this.schedulerClient = createSchedulerClient();

    this.policyConfigReader = createPolicyConfigReader();

    AbstractModule module = constructConfigModule(trackerURL);
    injector = Guice.createInjector(module);

    initializePolicies();
  }

  @SuppressWarnings("unchecked") // we don't know what T is until runtime
  private void initializePolicies() throws ClassNotFoundException {
    List<String> policyIds = policyConfigReader.getPolicyIds();
    for (String policyId : policyIds) {
      Map<String, String> policyConfigMap = policyConfigReader.getPolicyConfig(policyId);
      HealthPolicyConfig policyConfig = new HealthPolicyConfig(policyConfigMap);

      String policyClassName = policyConfig.getPolicyClass(policyId);
      LOG.info(String.format("Initializing %s with class %s", policyId, policyClassName));
      Class<IHealthPolicy> policyClass
          = (Class<IHealthPolicy>) this.getClass().getClassLoader().loadClass(policyClassName);

      AbstractModule module = constructPolicySpecificModule(policyConfig);
      IHealthPolicy policy = injector.createChildInjector(module).getInstance(policyClass);
      policy.initialize();

      healthPolicies.add(policy);
    }
  }

  @VisibleForTesting
  HealthPolicyConfigReader createPolicyConfigReader() throws FileNotFoundException {
    String policyConfigFile
        = Paths.get(Context.heronConf(config), HealthMgrConstants.CONF_FILE_NAME).toString();
    return new HealthPolicyConfigReader(policyConfigFile);
  }

  private AbstractModule constructConfigModule(final String trackerURL) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(String.class)
            .annotatedWith(Names.named(HealthMgrConstants.CONF_TRACKER_URL))
            .toInstance(trackerURL);
        bind(String.class)
            .annotatedWith(Names.named(HealthMgrConstants.CONF_TOPOLOGY_NAME))
            .toInstance(Context.topologyName(config));
        bind(String.class)
            .annotatedWith(Names.named(HealthMgrConstants.CONF_CLUSTER))
            .toInstance(Context.cluster(config));
        bind(String.class)
            .annotatedWith(Names.named(HealthMgrConstants.CONF_ENVIRON))
            .toInstance(Context.environ(config));
        bind(Config.class).toInstance(config);
        bind(EventManager.class).in(Singleton.class);

        bind(ISchedulerClient.class).toInstance(schedulerClient);
        bind(SchedulerStateManagerAdaptor.class).toInstance(stateMgrAdaptor);
        bind(MetricsProvider.class).to(TrackerMetricsProvider.class).in(Singleton.class);
        bind(PackingPlanProvider.class).in(Singleton.class);
      }
    };
  }

  private AbstractModule constructPolicySpecificModule(final HealthPolicyConfig policyConfig) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(HealthPolicyConfig.class).toInstance(policyConfig);
      }
    };
  }

  @VisibleForTesting
  SchedulerStateManagerAdaptor createStateMgrAdaptor() throws ReflectiveOperationException {
    String stateMgrClass = Context.stateManagerClass(config);
    IStateManager stateMgr = ReflectionUtils.newInstance(stateMgrClass);
    stateMgr.initialize(config);
    return new SchedulerStateManagerAdaptor(stateMgr, 5000);
  }

  private ISchedulerClient createSchedulerClient() {
    return new SchedulerClientFactory(config, runtime).getSchedulerClient();
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
  private static Config commandLineConfigs(String cluster,
                                           String role,
                                           String environ,
                                           String topologyName,
                                           Boolean verbose) {
    Config.Builder commandLineConfig = Config.newBuilder()
        .put(Key.CLUSTER, cluster)
        .put(Key.ROLE, role)
        .put(Key.ENVIRON, environ)
        .put(Key.TOPOLOGY_NAME, topologyName)
        .put(Key.VERBOSE, verbose);

    return commandLineConfig.build();
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(HealthManager.class.getSimpleName(), options);
  }

  // construct command line help options
  private static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  // Construct all required command line options
  private static Options constructCliOptions() {
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
        .build();

    Option heronHome = Option.builder("d")
        .desc("Directory where heron is installed")
        .longOpt("heron_home")
        .hasArgs()
        .argName("heron home dir")
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

    Option topologyName = Option.builder("n")
        .desc("Name of the topology")
        .longOpt("topology_name")
        .hasArgs()
        .argName("topology name")
        .required()
        .build();

    Option trackerURL = Option.builder("t")
        .desc("Tracker url with port number")
        .longOpt("tracker_url")
        .hasArgs()
        .argName("tracker url")
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(heronHome);
    options.addOption(configFile);
    options.addOption(configOverrides);
    options.addOption(topologyName);
    options.addOption(trackerURL);
    options.addOption(verbose);

    return options;
  }

  @VisibleForTesting
  List<IHealthPolicy> getHealthPolicies() {
    return healthPolicies;
  }
}