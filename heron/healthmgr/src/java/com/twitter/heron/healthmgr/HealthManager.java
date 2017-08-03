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

package com.twitter.heron.healthmgr;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.microsoft.dhalion.api.IHealthPolicy;
import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.PoliciesExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.classification.InterfaceStability.Evolving;
import com.twitter.heron.classification.InterfaceStability.Unstable;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey;
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
 * {@link HealthManager} makes a topology dynamic and self-regulating. This is implemented using
 * Dhalion library. The {@link HealthManager} will perform the following functions to achieve its
 * goal:
 * <ul>
 * <li>loads heron configuration including health policy configuration from
 * <code>healthmgr.yaml</code>
 * <li>initializing guice injector with metrics collection module from <code>tracker</code> or
 * <code>metrics cache</code>, <code>scheduler client</code> and <code>state client</code>
 * <li>initializes health policies instances and starts policy execution using
 * {@link PoliciesExecutor}
 * </ul>
 * The {@link HealthManager} is executed as a process. It is recommended that it is started on
 * container 0, colocated with the metrics provider and the scheduler service.
 * <p>
 * Required command line options for the {@link HealthManager} include
 * <ul>
 * <li>cluster name: <code>-c local</code>
 * <li>role: <code> -r dev</code>
 * <li>environment: <code> -e default</code>
 * <li>heron home directory: <code> -d ~/.heron</code>
 * <li>config directory: <code> -p ~/.heron/conf</code>
 * <li>topology name: <code> -n AckingTopology</code>
 * </ul>
 * <p>
 * Optional command line options for the {@link HealthManager} include
 * <ul>
 * <li>tracker url: <code>-t http://host:port</code>, default: <code>http://localhost:8888</code>
 * <li>enable verbose mode: <code> -v</code>
 * </ul>
 */
@Unstable
@Evolving
public class HealthManager {
  public static final String CONF_TOPOLOGY_NAME = "TOPOLOGY_NAME";
  public static final String CONF_METRICS_SOURCE_URL = "METRICS_SOURCE_URL";

  private static final Logger LOG = Logger.getLogger(HealthManager.class.getName());
  private final Config config;
  private AbstractModule baseModule;

  private Config runtime;
  private Injector injector;
  private SchedulerStateManagerAdaptor stateMgrAdaptor;
  private ISchedulerClient schedulerClient;

  private List<IHealthPolicy> healthPolicies = new ArrayList<>();
  private HealthPolicyConfigReader policyConfigReader;

  public HealthManager(Config config, AbstractModule baseModule) {
    this.config = config;
    this.baseModule = baseModule;
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

    String heronHome = cmd.getOptionValue("heron_home");
    String configPath = cmd.getOptionValue("config_path");
    String metricsSourceUrl = cmd.getOptionValue("data_source_url", "http://localhost:8888");
    String metricsProviderClassName = cmd.getOptionValue(
        "data_source_type", "com.twitter.heron.healthmgr.sensors.TrackerMetricsProvider");

    Boolean verbose = cmd.hasOption("verbose");
    Level loggingLevel = Level.INFO;
    if (verbose) {
      loggingLevel = Level.FINE;
    }
    LoggingHelper.loggerInit(loggingLevel, false);

    // build the final config by expanding all the variables
    Config config = Config.toLocalMode(Config.newBuilder()
        .putAll(ConfigLoader.loadConfig(heronHome, configPath, null, null))
        .putAll(commandLineConfigs(cmd))
        .build());

    LOG.info("Static Heron config loaded successfully ");
    LOG.fine(config.toString());

    Class<? extends MetricsProvider> metricsProviderClass =
        Class.forName(metricsProviderClassName).asSubclass(MetricsProvider.class);
    AbstractModule module =
        buildMetricsProviderModule(config, metricsSourceUrl, metricsProviderClass);
    HealthManager healthManager = new HealthManager(config, module);

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

    injector = Guice.createInjector(baseModule);
    AbstractModule commonModule = buildCommonConfigModule();
    injector = injector.createChildInjector(commonModule);

    initializePolicies();
  }

  @SuppressWarnings("unchecked") // we don't know what T is until runtime
  private void initializePolicies() throws ClassNotFoundException {
    List<String> policyIds = policyConfigReader.getPolicyIds();
    for (String policyId : policyIds) {
      Map<String, Object> policyConfigMap = policyConfigReader.getPolicyConfig(policyId);
      HealthPolicyConfig policyConfig = new HealthPolicyConfig(policyConfigMap);

      String policyClassName = policyConfig.getPolicyClass();
      LOG.info(String.format("Initializing %s with class %s", policyId, policyClassName));
      Class<IHealthPolicy> policyClass
          = (Class<IHealthPolicy>) this.getClass().getClassLoader().loadClass(policyClassName);

      AbstractModule module = constructPolicySpecificModule(policyConfig);
      IHealthPolicy policy = injector.createChildInjector(module).getInstance(policyClass);

      healthPolicies.add(policy);
    }
  }

  @VisibleForTesting
  HealthPolicyConfigReader createPolicyConfigReader() throws FileNotFoundException {
    String policyConfigFile
        = Paths.get(Context.heronConf(config), PolicyConfigKey.CONF_FILE_NAME.key()).toString();
    HealthPolicyConfigReader configReader = new HealthPolicyConfigReader(policyConfigFile);
    configReader.loadConfig();
    return configReader;
  }

  @VisibleForTesting
  static AbstractModule buildMetricsProviderModule(
      final Config config, final String metricsSourceUrl,
      final Class<? extends MetricsProvider> metricsProviderClass) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(String.class)
            .annotatedWith(Names.named(CONF_METRICS_SOURCE_URL))
            .toInstance(metricsSourceUrl);
        bind(String.class)
            .annotatedWith(Names.named(CONF_TOPOLOGY_NAME))
            .toInstance(Context.topologyName(config));
        bind(String.class)
            .annotatedWith(Names.named(TrackerMetricsProvider.CONF_CLUSTER))
            .toInstance(Context.cluster(config));
        bind(String.class)
            .annotatedWith(Names.named(TrackerMetricsProvider.CONF_ENVIRON))
            .toInstance(Context.environ(config));
        bind(MetricsProvider.class).to(metricsProviderClass).in(Singleton.class);
      }
    };
  }

  private AbstractModule buildCommonConfigModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Config.class).toInstance(config);
        bind(EventManager.class).in(Singleton.class);
        bind(ISchedulerClient.class).toInstance(schedulerClient);
        bind(SchedulerStateManagerAdaptor.class).toInstance(stateMgrAdaptor);
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
   * @param cmd command line options
   * @return config, the command line config
   */
  private static Config commandLineConfigs(CommandLine cmd) {
    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    String topologyName = cmd.getOptionValue("topology_name");
    Boolean verbose = cmd.hasOption("verbose");

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

    Option topologyName = Option.builder("n")
        .desc("Name of the topology")
        .longOpt("topology_name")
        .hasArgs()
        .argName("topology name")
        .required()
        .build();

    Option metricsSourceURL = Option.builder("t")
        .desc("metrics data source url with port number")
        .longOpt("data_source_url")
        .hasArgs()
        .argName("data source url")
        .build();

    // candidate metrics sources are:
    // com.twitter.heron.healthmgr.sensors.TrackerMetricsProvider (default)
    // com.twitter.heron.healthmgr.sensors.MetricsCacheMetricsProvider
    Option metricsSourceType = Option.builder("s")
        .desc("metrics data source type")
        .longOpt("data_source_type")
        .hasArg()
        .argName("data source type")
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
    options.addOption(topologyName);
    options.addOption(metricsSourceType);
    options.addOption(metricsSourceURL);
    options.addOption(verbose);

    return options;
  }

  @VisibleForTesting
  List<IHealthPolicy> getHealthPolicies() {
    return healthPolicies;
  }
}
