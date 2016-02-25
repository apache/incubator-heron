package com.twitter.heron.scheduler.local;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.system.ExecutionEnvironment;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.common.HttpUtils;

import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;

import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.IStateManager;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Launch topology locally to a working directory.
 */
public class LocalLauncher implements ILauncher {
  protected static final Logger LOG = Logger.getLogger(LocalLauncher.class.getName());

  private Config config;
  private Config runtime;

  private TopologyAPI.Topology topology;
  private String topologyWorkingDirectory;
  private String coreReleasePackage;

  @Override
  public void initialize(Config config, Config runtime) {

    this.config = config;
    this.runtime = runtime;

    // get the topology working directory
    this.topologyWorkingDirectory = LocalContext.workingDirectory(config);

    // get the path of core release URI 
    this.coreReleasePackage = LocalContext.corePackageUri(config);
  }

  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));
    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  /** 
   * Pack topology related configs
   */
  protected Config topologyConfigs() {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    Config.Builder builder = Config.newBuilder()
        .put(Keys.TOPOLOGY_ID, topology.getId())
        .put(Keys.TOPOLOGY_NAME, topology.getName())
        .put(Keys.TOPOLOGY_DEFINITION_FILE, Context.topologyDefinitionFile(config))
        .put(Keys.TOPOLOGY_PACKAGE_URI, Context.topologyPackageUri(config))
        .put(Keys.TOPOLOGY_JAR_FILE, Context.topologyJarFile(config))
        .put(Keys.TOPOLOGY_PACKAGE_TYPE, Context.topologyPackageType(config));
    return builder.build();
  }

  /** 
   * Pack component related configs
   */
  protected Config componentConfigs() {
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    String jvmOptions = TopologyUtils.getComponentJvmOptions(topology);
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMap(topology);

    Config.Builder builder = Config.newBuilder()
        .put(LocalSchedulerKeys.COMPONENT_RAMMAP, TopologyUtils.formatRamMap(ramMap))
        .put(LocalSchedulerKeys.COMPONENT_JVM_OPTS_IN_BASE64, formatJavaOpts(jvmOptions));
    return builder.build();
  }

  /** 
   * Pack instance related configs
   */
  protected Config instanceConfigs(PackingPlan packing) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    String distribution = TopologyUtils.packingToString(packing);
    String jvmOptions = TopologyUtils.getInstanceJvmOptions(topology);

    Config.Builder builder = Config.newBuilder()
        .put(LocalSchedulerKeys.INSTANCE_DISTRIBUTION, distribution)
        .put(LocalSchedulerKeys.INSTANCE_JVM_OPTS_IN_BASE64, formatJavaOpts(jvmOptions));
    return builder.build(); 
  }

  /** 
   * Pack resource related configs
   */
  protected Config resourceConfigs() {
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    int numShards = 1 + TopologyUtils.getNumContainers(topology);

    Config.Builder builder = Config.newBuilder()
        .put(LocalSchedulerKeys.NUM_SHARDS, String.valueOf(numShards));
    return builder.build();
  }

  /** 
   * Pack configs related to files and directories
   */
  protected Config filesAndDirsConfigs() {
    Config.Builder builder = Config.newBuilder();
    builder.put(LocalSchedulerKeys.WORKING_DIRECTORY, topologyWorkingDirectory);
    return builder.build();
  }

  /** 
   * Actions to execute before launch such as
   *
   *   check whether the topology is already running
   *
   */
  @Override
  public boolean prepareLaunch(PackingPlan packing) {
    LOG.info("Checking whether the topology has been launched already!");

    String topologyName = Context.topologyName(config);
    IStateManager stateManager = Runtime.stateManager(runtime);

    // check if any topology with the same name is running
    ListenableFuture<Boolean> boolFuture = stateManager.isTopologyRunning(topologyName);
    if (NetworkUtils.awaitResult(boolFuture, 1000, TimeUnit.MILLISECONDS)) {
      LOG.severe("Topology is already running: " + topologyName);
      return false;
    }
    return true;
  }

  /** 
   * Launch the topology 
   */
  @Override
  public boolean launch(PackingPlan packing) {
    LOG.info("Launching topology for local cluster " + Context.cluster(config));

    // get all the config - need to be passed as command line to heron executor
    Config localConfig = Config.newBuilder()
        .putAll(topologyConfigs())
        .putAll(componentConfigs())
        .putAll(instanceConfigs(packing))
        .putAll(resourceConfigs())
        .putAll(filesAndDirsConfigs())
        .build();

    // download the core and topology packages into the working directory
    if (!downloadPackages()) {
      LOG.severe("Failed to download the core and topology packages");
      return false;
    }

    String configInBase64 =
        DatatypeConverter.printBase64Binary(localConfig.asString().getBytes(Charset.forName("UTF-8")));

    // form the scheduler path (TO DO: Karthik change to libs directory)
    String schedulerBinary = Paths.get(topologyWorkingDirectory, Context.schedulerJar(config)).toString(); 

    StringBuilder schedulerCmd = new StringBuilder()
        .append("java").append(" ")
        .append("-cp").append(" ")
        .append(schedulerBinary).append(" ")
        .append("com.twitter.heron.scheduler.service.SchedulerMain").append(" ")
        .append(Runtime.topologyName(runtime)).append(" ")
        .append(Context.schedulerClass(config)).append(" ")
        .append(configInBase64).append(" ")
        .append(NetworkUtils.getFreePort()).append(" ");
    LOG.info("Local scheduler command line: " + schedulerCmd.toString());

    return 0 == ShellUtils.runSyncProcess(true, true, schedulerCmd.toString(),
        new StringBuilder(), new StringBuilder(), new File(topologyWorkingDirectory));
  }

  @Override
  public boolean postLaunch(PackingPlan packing) {
    return true;
  }

  @Override
  public void undo() {
    // Currently nothing need to do here
  }

  @Override
  public ExecutionEnvironment.ExecutionState updateExecutionState(
      ExecutionEnvironment.ExecutionState executionState) {
    String release = "local-live-release";

    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder().mergeFrom(executionState);

    builder.setDc(Context.cluster(config))
        .setCluster(Context.cluster(config))
        .setRole(Context.role(config))
        .setEnviron(Context.environ(config));

    // Set the HeronReleaseState
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();
    releaseBuilder.setReleaseUsername(Context.role(config));
    releaseBuilder.setReleaseTag(release);
    releaseBuilder.setReleaseVersion(release);
    releaseBuilder.setUploaderVersion(release);

    builder.setReleaseState(releaseBuilder);

    if (!builder.isInitialized()) {
      throw new RuntimeException("Failed to create execution state");
    }

    return builder.build();
  }

  /**
   * Download heron core and the topology packages into topology working directory
   *
   * @return true if successful
   */
  protected boolean downloadPackages() {
    
    // log the state manager being used - for visibility and debugging purposes
    IStateManager stateManager = Runtime.stateManager(runtime);
    LOG.info("State manager used: " + stateManager.getClass().getName());

    // copy the heron core release package to the working directory and untar it
    LOG.info("Fetching heron core release " + coreReleasePackage + " and untarred");
    LOG.info("If release package already in working directory, the old one will be overwritten");
    if (!untarPackage(coreReleasePackage, topologyWorkingDirectory)) {
      LOG.severe("Failed to fetch and untar heron core release package.");
      return false;
    }

    // untar the topology package
    String topologyPackage = Context.topologyPackageUri(config);
    LOG.info("Untar the topology package: " + topologyPackage);

    if (!untarPackage(topologyPackage, topologyWorkingDirectory)) {
      LOG.severe("Failed to untar topology package.");
      return false;
    }

    return true;
  }

  /**
   * Untar a tar package to a target folder
   *
   * @param packageName the tar package
   * @param targetFolder the target folder
   * @return true if untar successfully
   */
  protected boolean untarPackage(String packageName, String targetFolder) {
    String cmd = String.format("tar -xvf %s", packageName);

    int ret = ShellUtils.runSyncProcess(false, true, cmd,
        new StringBuilder(), new StringBuilder(), new File(targetFolder));

    return ret == 0 ? true : false;
  }
}
