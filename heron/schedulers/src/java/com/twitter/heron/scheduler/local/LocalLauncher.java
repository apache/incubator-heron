package com.twitter.heron.scheduler.local;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Defaults;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.common.HttpUtils;

import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;

import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.IStateManager;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

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
  private String targetCoreReleaseFile;
  private String targetTopologyPackageFile;

  @Override
  public void initialize(Config config, Config runtime) {

    this.config = config;
    this.runtime = runtime;

    // get the topology working directory
    this.topologyWorkingDirectory = LocalContext.workingDirectory(config);

    // get the path of core release URI 
    this.coreReleasePackage = Context.corePackageUri(config);

    // form the target dest core release file name
    this.targetCoreReleaseFile = Paths.get(
        topologyWorkingDirectory, "heron-core.tar.gz").toString();

    // form the target topology package file name
    this.targetTopologyPackageFile = Paths.get(
        topologyWorkingDirectory, "topology.tar.gz").toString();
  }

  /**
   * Encode the JVM options
   *
   * @return encoded string
   */
  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  /** 
   * Pack the default configs into a Config
   *
   * @return Config 
   */
  protected Config defaultConfigs() {
    Config.Builder builder = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaultHome())
        .putAll(ClusterDefaults.getDefaultBinaries())
        .putAll(ClusterDefaults.getDefaultJars())
        .putAll(ClusterConfig.loadSchedulerConfig());
    return builder.build();
  }

  /** 
   * Pack topology related configs into a Config
   *
   * @return Config 
   */
  protected Config topologyConfigs() {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    // get topology attributes, for files get the base name
    String defFile = FilenameUtils.getName(Context.topologyDefinitionFile(config));
    String jarFile = FilenameUtils.getName(Context.topologyJarFile(config));
    String pkgType = Context.topologyPackageType(config);

    // build the config 
    Config.Builder builder = Config.newBuilder()
        .put(Keys.TOPOLOGY_ID, topology.getId())
        .put(Keys.TOPOLOGY_NAME, topology.getName())
        .put(Keys.TOPOLOGY_DEFINITION_FILE, defFile)
        .put(Keys.TOPOLOGY_JAR_FILE, jarFile)
        .put(Keys.TOPOLOGY_PACKAGE_TYPE, pkgType);

    return builder.build();
  }

  /** 
   * Pack topology component related configs into a Config
   *
   * @return Config
   */
  protected Config componentConfigs() {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    // get the jvm options for the components supplied topology writer 
    String jvmOptions = TopologyUtils.getComponentJvmOptions(topology);

    // get the component RAM map specified by the topology writer 
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMap(topology);

    // build the config  
    Config.Builder builder = Config.newBuilder()
        .put(Keys.COMPONENT_RAMMAP, TopologyUtils.formatRamMap(ramMap))
        .put(Keys.COMPONENT_JVM_OPTS_IN_BASE64, formatJavaOpts(jvmOptions));

    return builder.build();
  }

  /** 
   * Pack instance related configs into a Config
   *
   * @return Config
   */
  protected Config instanceConfigs(PackingPlan packing) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    // get the instance distribution for the packing
    String distribution = TopologyUtils.packingToString(packing);

    // get instance JVM options specified by topology writer
    String jvmOptions = TopologyUtils.getInstanceJvmOptions(topology);

    // build the config
    Config.Builder builder = Config.newBuilder()
        .put(Keys.INSTANCE_DISTRIBUTION, distribution)
        .put(Keys.INSTANCE_JVM_OPTS_IN_BASE64, formatJavaOpts(jvmOptions));

    return builder.build(); 
  }

  /** 
   * Pack resource related configs into a Config
   *
   * @return Config
   */
  protected Config resourceConfigs() {
    TopologyAPI.Topology topology = Runtime.topology(runtime);
 
    // get the number of containers, the extra one is for topology master
    int numContainers = 1 + TopologyUtils.getNumContainers(topology);

    // build the config
    Config.Builder builder = Config.newBuilder()
        .put(Keys.NUM_CONTAINERS, String.valueOf(numContainers));

    return builder.build();
  }

  /** 
   * Pack configs related to files and directories into a Config
   *
   * @return Config
   */
  protected Config filesAndDirsConfigs() {
    Config.Builder builder = Config.newBuilder()
        .put(Keys.LOGGING_DIRECTORY, Defaults.LOGGING_DIRECTORY)
        .put(LocalKeys.WORKING_DIRECTORY, topologyWorkingDirectory);

    return builder.build();
  }

  protected Config binaryConfigs() {
    Config.Builder builder = Config.newBuilder()
        .put(LocalKeys.WORKING_DIRECTORY, topologyWorkingDirectory);

    return builder.build();
  }


  /** 
   * Actions to execute before launch such as check whether the
   * topology is already running
   *
   * @return true, if successful
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

    // get all the config, need to be passed as command line to heron executor
    Config schedulerConfig = Config.expand(
        Config.newBuilder()
            .putAll(defaultConfigs())
            .putAll(topologyConfigs())
            .putAll(componentConfigs())
            .putAll(instanceConfigs(packing))
            .putAll(resourceConfigs())
            .putAll(filesAndDirsConfigs())
            .build());

    LOG.info("loaded scheduler config " + schedulerConfig);

    // download the core and topology packages into the working directory
    if (!downloadAndExtractPackages()) {
      LOG.severe("Failed to download the core and topology packages");
      return false;
    }

    String configInBase64 =
       DatatypeConverter.printBase64Binary(schedulerConfig.asString().getBytes(Charset.forName("UTF-8")));

    System.out.println(configInBase64);
    System.exit(0);

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
    String release = "local-live";

    // build the heron release state
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();

    releaseBuilder.setReleaseUsername(Context.role(config));
    releaseBuilder.setReleaseTag(release);
    releaseBuilder.setReleaseVersion(release);
    releaseBuilder.setUploaderVersion(release);

    // build the execution state
    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder();

    builder.mergeFrom(executionState)
        .setDc(Context.cluster(config))
        .setCluster(Context.cluster(config))
        .setRole(Context.role(config))
        .setEnviron(Context.environ(config))
        .setReleaseState(releaseBuilder);

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
  protected boolean downloadAndExtractPackages() {
    
    // log the state manager being used, for visibility and debugging purposes
    IStateManager stateManager = Runtime.stateManager(runtime);
    LOG.info("State manager used: " + stateManager.getClass().getName());

    // if the working directory does not exist, create it.
    File workingDirectory = new File(topologyWorkingDirectory); 
    if (!workingDirectory.exists()) {
      LOG.info("The working directory does not exist; creating it.");
      if (!workingDirectory.mkdirs()) {
        LOG.severe("Failed to create directory: " + workingDirectory.getPath());
        return false;
      }
    }

    // copy the heron core release package to the working directory and untar it
    LOG.info("Fetching heron core release " + coreReleasePackage);
    LOG.info("If release package is already in the working directory");
    LOG.info("the old one will be overwritten");
    if (!copyPackage(coreReleasePackage, targetCoreReleaseFile)) {
      LOG.severe("Failed to fetch the heron core release package.");
      return false;
    }

    // untar the heron core release package in the working directory
    LOG.info("Untar the heron core release " + coreReleasePackage);
    if (!untarPackage(targetCoreReleaseFile, topologyWorkingDirectory)) {
      LOG.severe("Failed to untar heron core release package.");
      return false;
    }

    // remove the core release package
    if (!FileUtils.deleteQuietly(new File(targetCoreReleaseFile))) {
      LOG.warning("Unable to delete the core release file: " + targetCoreReleaseFile);
    }

    // fetch the topology package
    String topologyPackage = Runtime.topologyPackageUri(runtime);
    LOG.info("Fetching topology package " + Runtime.topologyPackageUri(runtime));
    LOG.info("If topology package is already in the working directory");
    LOG.info("the old one will be overwritten");
    if (!copyPackage(topologyPackage, targetTopologyPackageFile)) {
      LOG.severe("Failed to fetch the heron core release package.");
      return false;
    }
    
    // untar the topology package
    LOG.info("Untar the topology package: " + topologyPackage);

    if (!untarPackage(targetTopologyPackageFile, topologyWorkingDirectory)) {
      LOG.severe("Failed to untar topology package.");
      return false;
    }

    // remove the topology package
    if (!FileUtils.deleteQuietly(new File(targetTopologyPackageFile))) {
      LOG.warning("Unable to delete the core release file: " + targetTopologyPackageFile);
    }

    return true;
  }

  /**
   * Copy a URL package to a target folder
   *
   * @param packageName the tar package
   * @param targetFolder the target folder
   * @return true if untar successfully
   */
  protected boolean copyPackage(String corePackageUrl, String targetFile) {

    // get the directory containing the target file
    Path filePath = Paths.get(targetFile);
    File parentDirectory = filePath.getParent().toFile();

    // using curl copy the url to the target file
    String cmd = String.format("curl %s -o %s", corePackageUrl, targetFile);
    int ret = ShellUtils.runSyncProcess(false, true, cmd,
        new StringBuilder(), new StringBuilder(), parentDirectory);

    return ret == 0 ? true : false;
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
