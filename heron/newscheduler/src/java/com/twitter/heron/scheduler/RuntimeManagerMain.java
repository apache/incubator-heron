package com.twitter.heron.scheduler;

import java.nio.charset.Charset;
import java.util.logging.Logger;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;

import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;

import com.google.common.util.concurrent.ListenableFuture;

public class RuntimeManagerMain {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerMain.class.getName());

  public static void main(String[] args)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {

    String cluster = args[0];
    String role = args[1];
    String environ = args[2];
    String heronHome = args[3];
    String configPath = args[4];
    String configOverrideEncoded = args[5];
    String topologyName = args[6];
    String command = args[7];

    // first load the defaults, then the config from files to override it
    Config.Builder defaultsConfig = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, configPath));

    // add config parameters from the command line
    Config.Builder commandLineConfig = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ);

    Config.Builder topologyConfig = Config.newBuilder()
        .put(Keys.topologyName(), topologyName);

    // TODO (Karthik) override any parameters from the command line

    // build the final config by expanding all the variables
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultsConfig.build())
            .putAll(commandLineConfig.build())
            .putAll(topologyConfig.build())
            .build());

    LOG.info("Static config loaded successfully ");
    LOG.info(config.toString());

    // invoke the appropriate command to manage the topology
    LOG.info("Topology: " + topologyName + " to be " + command + "ed");
    boolean rt = manageTopology(config, command);
    if (!rt) {
      LOG.severe("Failed to " + command + " topology " + topologyName);
      Runtime.getRuntime().exit(1);
    }

    LOG.info("Topology " + topologyName + " " + command + " successfully");
  }

  public static boolean manageTopology(Config config, String command)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {

    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = (IStateManager)Class.forName(statemgrClass).newInstance();

    // initialize the statemgr
    statemgr.initialize(config);

    // build the runtime config
    Config runtime = Config.newBuilder()
        .put(Keys.topologyName(), Context.topologyName(config))
        .put(Keys.schedulerStateManagerAdaptor(), new SchedulerStateManagerAdaptor(statemgr))
        .build();

    // create an instance of the runner class
    RuntimeManagerRunner runtimeManagerRunner = new RuntimeManagerRunner(config, runtime, command);

    // invoke the appropriate handlers based on command
    boolean ret = runtimeManagerRunner.call();

    // close the state manager 
    statemgr.close();
    return ret;
  }
}
