package com.twitter.heron.scheduler;

import java.nio.charset.Charset;
import java.util.logging.Logger;
import java.io.IOException;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.statemgr.IStateManager;

public class RuntimeManagerMain {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerMain.class.getName());

  public static void main(String[] args)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {

    String cluster = args[0];
    String role = args[1];
    String environ = args[2];
    String heronHome = args[3];
    String topologyName = args[4];
    String command = args[5];

    // first load the defaults, then the config from files to override it
    Config.Builder defaultConfigs = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, cluster));

    // add config parameters from the command line
    Config.Builder commandLineConfigs = Config.newBuilder()
        .put(Keys.CLUSTER, cluster)
        .put(Keys.ROLE, role)
        .put(Keys.ENVIRON, environ);

    Config.Builder topologyConfigs = Config.newBuilder()
        .put(Keys.TOPOLOGY_NAME, topologyName);

    Config config = Config.newBuilder()
        .putAll(defaultConfigs.build())
        .putAll(commandLineConfigs.build())
        .putAll(topologyConfigs.build())
        .build();

    boolean rt = manageTopology(config, topologyName, command);
    if (!rt) {
      LOG.severe("Failed to " + command + " topology " + topologyName);
      Runtime.getRuntime().exit(1);
    }

    LOG.info("Topology " + topologyName + " " + command + " successfully");
  }

  public static boolean manageTopology(Config config, String topologyName, String command)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {

    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = (IStateManager)Class.forName(statemgrClass).newInstance();
    // TODO - initialize the statemgr everywhere

    // build the runtime config
    Config runtime = Config.newBuilder()
        .put(Keys.TOPOLOGY_NAME, topologyName)
        .put(Keys.STATE_MANAGER, statemgr)
        .build();

    // create an instance of the runner class
    RuntimeManagerRunner runtimeManagerRunner = new RuntimeManagerRunner(config, runtime, command);

    // invoke the appropriate handlers based on command
    boolean ret = runtimeManagerRunner.call();

    // TODO close the state manager 
    return ret;
  }
}
