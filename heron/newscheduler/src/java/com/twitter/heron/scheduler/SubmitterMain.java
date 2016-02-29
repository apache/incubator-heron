package com.twitter.heron.scheduler;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;

import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.utils.TopologyUtils;

import com.twitter.heron.api.generated.TopologyAPI;

/**
 * Calls Uploader to upload topology package, and Launcher to launch Scheduler.
 * TODO(nbhagat): Use commons cli to parse command line.
 * TODO(nbhagat): Make all argument passed to CLI available here.
 * args[0] = Topology Package location. Topology package generated from heron-cli will be a
 * tar file containing topology, topology definition and all dependencies.
 * args[1] = ConfigLoader class.
 * args[2] = Scheduler config file.
 * args[3] = Config override string encoded as base64 string.
 * args[4] = topology definition file.
 * args[5] = Heron internals config file location.
 * args[6] = Original package.
 */
public class SubmitterMain {
  private static final Logger LOG = Logger.getLogger(SubmitterMain.class.getName());

  public static void main(String[] args) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    String cluster = args[0];
    String role = args[1];
    String environ = args[2];
    String heronHome = args[3];
    String configPath = args[4];
    String configOverrideEncoded = args[5];

    String topologyPackage = args[6];
    String topologyDefnFile = args[7];
    String originalPackageFile = args[8];

    // First load the defaults, then the config from files to override it 
    Config.Builder defaultsConfig = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, cluster, configPath));
 
    // Add config parameters from the command line
    Config.Builder commandLineConfig = Config.newBuilder()
        .put(Keys.CLUSTER, cluster)
        .put(Keys.ROLE, role)
        .put(Keys.ENVIRON, environ);

    // Identify the type of topology package
    String pkgType = FileUtils.isOriginalPackageJar(
        FileUtils.getBaseName(originalPackageFile)) ? "jar" : "tar" ; 

    // Load the topology definition into topology proto
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    Config.Builder topologyConfig = Config.newBuilder()
        .put(Keys.TOPOLOGY_ID, topology.getId())
        .put(Keys.TOPOLOGY_NAME, topology.getName())
        .put(Keys.TOPOLOGY_DEFINITION_FILE, topologyDefnFile)
        .put(Keys.TOPOLOGY_PACKAGE_FILE, topologyPackage)
        .put(Keys.TOPOLOGY_JAR_FILE, originalPackageFile)
        .put(Keys.TOPOLOGY_PACKAGE_TYPE, pkgType);

    // TODO (Karthik) override any parameters from the command line

    // Build the final config by expanding all the variables
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultsConfig.build())
            .putAll(commandLineConfig.build())
            .putAll(topologyConfig.build())
            .build());

    LOG.info("loaded static config " + config);

    // submit the topology with the given config
    if (!submitTopology(config, topology)) {
      Runtime.getRuntime().exit(1);
    }
  }
 
  public static boolean submitTopology(Config config, TopologyAPI.Topology topology) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = (IStateManager)Class.forName(statemgrClass).newInstance();

    // initialize the state manager
    statemgr.initialize(config);

    // upload the topology package to the storage
    UploadRunner uploadRunner = new UploadRunner(config);
    boolean result = uploadRunner.call();

    if (!result) {
      LOG.severe("Failed to upload package. Exiting");
      statemgr.close();
      return false;
    }

    // build the runtime config
    Config runtime = Config.newBuilder()
        .put(Keys.TOPOLOGY_ID, topology.getId())
        .put(Keys.TOPOLOGY_NAME, topology.getName())
        .put(Keys.TOPOLOGY_DEFINITION, topology)
        .put(Keys.STATE_MANAGER, statemgr)
        .put(Keys.TOPOLOGY_PACKAGE_URI, uploadRunner.getUri())
        .build();

    // using launch runner, launch the topology
    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    result = launchRunner.call();

    // if failed, undo the uploaded package
    if (!result) {
      LOG.severe("Failed to launch topology. Attempting to roll back upload.");
      uploadRunner.undo();
      statemgr.close();
      return false;
    }

    // close the statemanager
    statemgr.close();

    return true;
  }
}
