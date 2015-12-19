package com.twitter.heron.scheduler.local;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.core.base.FileUtility;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.ILauncher;
import com.twitter.heron.scheduler.api.PackingPlan;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.service.SubmitterMain;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.util.ShellUtility;
import com.twitter.heron.scheduler.util.TopologyUtility;

/**
 * Launch topology locally to a working directory.
 */
public class LocalLauncher implements ILauncher {
  protected static final Logger LOG = Logger.getLogger(LocalLauncher.class.getName());

  //  private LocalConfig schedulerConfig;
  private LaunchContext context;
  private LocalConfig localConfig;
  private TopologyAPI.Topology topology;

  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));
    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  @Override
  public void initialize(LaunchContext context) {
    this.context = context;
    this.topology = context.getTopology();
    this.localConfig = new LocalConfig(context);
  }

  @Override
  public boolean prepareLaunch(PackingPlan packing) {
    LOG.info("Checking whether the topology has been launched already!");

    if (NetworkUtility.awaitResult(context.getStateManagerAdaptor().isTopologyRunning(), 1000, TimeUnit.MILLISECONDS)) {
      LOG.severe("Topology has been running: " + topology.getName());
      return false;
    }
    return true;
  }

  @Override
  public boolean launchTopology(PackingPlan packing) {
    LOG.info("Launching topology in local cluster");
    Map<String, String> localProperties = new HashMap<String, String>();

    localProperties.put(LocalConfig.CLASS_PATH, TopologyUtility.makeClasspath(topology));
    localProperties.put(LocalConfig.COMPONENT_JVM_OPTS_IN_BASE64,
        formatJavaOpts(TopologyUtility.getComponentJvmOptions(topology)));
    localProperties.put(LocalConfig.COMPONENT_RAMMAP,
        TopologyUtility.formatRamMap(TopologyUtility.getComponentRamMap(topology)));
    localProperties.put(LocalConfig.HERON_EXECUTOR_BINARY, "heron-executor");
    localProperties.put(LocalConfig.HERON_SCHEDULER_BINARY, "heron-scheduler.jar");
    localProperties.put(LocalConfig.HERON_INTERNALS_CONFIG_FILENAME,
        FileUtility.getBaseName(SubmitterMain.getHeronInternalsConfigFile()));
    localProperties.put(LocalConfig.INSTANCE_DISTRIBUTION,
        TopologyUtility.packingToString(packing));
    localProperties.put(LocalConfig.INSTANCE_JVM_OPTS_IN_BASE64,
        formatJavaOpts(TopologyUtility.getInstanceJvmOptions(topology)));
    localProperties.put(LocalConfig.METRICS_MGR_CLASSPATH, "metrics-mgr-classpath/*");
    localProperties.put(LocalConfig.NUM_SHARDS,
        "" + (1 + TopologyUtility.getNumContainer(topology)));
    localProperties.put(LocalConfig.PKG_TYPE,
        (FileUtility.isOriginalPackageJar(
            FileUtility.getBaseName(SubmitterMain.getOriginalPackageFile())) ? "jar" : "tar"));
    localProperties.put(LocalConfig.STMGR_BINARY, "heron-stmgr");
    localProperties.put(LocalConfig.TMASTER_BINARY, "heron-tmaster");
    localProperties.put(LocalConfig.HERON_SHELL_BINARY, "heron-shell");
    localProperties.put(Constants.TOPOLOGY_DEFINITION_FILE, topology.getName() + ".defn");
    localProperties.put(LocalConfig.TOPOLOGY_ID, topology.getId());
    localProperties.put(LocalConfig.TOPOLOGY_JAR_FILE,
        FileUtility.getBaseName(SubmitterMain.getOriginalPackageFile()));
    localProperties.put(LocalConfig.TOPOLOGY_NAME, topology.getName());

    localProperties.put(LocalConfig.LOG_DIR, localConfig.getLogDir());
    localProperties.put(LocalConfig.WORKING_DIRECTORY, localConfig.getWorkingDirectory());
    localProperties.put(LocalConfig.HERON_JAVA_HOME, localConfig.getJavaHome());
    localProperties.put(LocalConfig.HERON_CORE_RELEASE_PACKAGE,
        localConfig.getHeronCoreReleasePackage());

    if (!localSetup(localConfig)) {
      LOG.severe("Failed to complete the local setup...");
      return false;
    }

    // Also pass config from Launcher to scheduler.
    for (Map.Entry<Object, Object> entry : context.getConfig().entrySet()) {
      String key = (String) entry.getKey();
      if (!localProperties.containsKey(key)) {
        localProperties.put(key, (String) entry.getValue());
      }
    }

    // Run the scheduler
    StringBuilder localSchedulerConfig = new StringBuilder();
    for (Map.Entry<String, String> kv : localProperties.entrySet()) {
      localSchedulerConfig.append(String.format(" %s=\"%s\" ", kv.getKey(), kv.getValue()));
    }

    String configInBase64 =
        DatatypeConverter.printBase64Binary(
            localSchedulerConfig.toString().getBytes(Charset.forName("UTF-8")));

    String schedulerCmd = String.format("%s %s %s %s %s %s %s %s %d",
        "java",
        "-cp",
        localConfig.getWorkingDirectory() + "/" +
            localProperties.get(LocalConfig.HERON_SCHEDULER_BINARY),
        "com.twitter.heron.scheduler.service.SchedulerMain",
        topology.getName(),
        context.getSchedulerClass(),
        DefaultConfigLoader.class.getName(),
        configInBase64,
        NetworkUtility.getFreePort());

    return 0 == ShellUtility.runSyncProcess(true, true, schedulerCmd,
        new StringBuilder(), new StringBuilder(),
        new File(localConfig.getWorkingDirectory()));
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
    // In LocalLauncher, we would supply with a dummy ExecutionState
    String dc = "local-dc";
    String role = "local-heron";
    String env = "local-environment";
    String release = "local-live-release";

    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder().mergeFrom(executionState);

    builder.setDc(dc).setRole(role).setEnviron(env);

    // Set the HeronReleaseState
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();
    releaseBuilder.setReleaseUsername(role);
    releaseBuilder.setReleaseTag(release);
    releaseBuilder.setReleaseVersion(release);
    releaseBuilder.setUploaderVersion(release);

    builder.setReleaseState(releaseBuilder);

    if (builder.isInitialized()) {
      return builder.build();
    } else {
      throw new RuntimeException("Failed to create execution state");
    }
  }

  protected boolean localSetup(LocalConfig config) {
    LOG.info("Is ZkStateManager used: " + config.isZkUsedForStateManager());
    // fetch the heron core release and untar it
    if (config.getHeronCoreReleasePackage() != null) {
      LOG.info("Fetching the heron core release and untar it: " +
          config.getHeronCoreReleasePackage());
      LOG.info("If release package already in working directory, the old one would be overwritten");
      if (!untarPackage(
          config.getHeronCoreReleasePackage(),
          config.getWorkingDirectory())) {
        LOG.severe("Failed to fetch and untar heron core release package.");
        return false;
      }
    } else {
      LOG.info("The heron core release package is not set; " +
          "supposing it is already in working directory");
    }

    // untar the topology
    LOG.info("Untar the topology package: " + config.getTopologyPackage());
    if (!untarPackage(
        config.getTopologyPackage(),
        config.getWorkingDirectory())) {
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

    int ret = ShellUtility.runSyncProcess(false, true, cmd,
        new StringBuilder(), new StringBuilder(), new File(targetFolder));

    return ret == 0 ? true : false;
  }
}
