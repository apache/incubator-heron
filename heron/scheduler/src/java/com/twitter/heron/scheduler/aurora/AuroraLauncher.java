package com.twitter.heron.scheduler.aurora;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.twitter.heron.scheduler.twitter.PackerUploader;
import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.twitter.PackerUtility;
import com.twitter.heron.scheduler.util.ShellUtility;
import com.twitter.heron.scheduler.util.TopologyUtility;
import com.twitter.heron.state.curator.CuratorStateManager;

/**
 * Launch topology on aurora.
 */
public class AuroraLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  public static final String HERON_DIR = "heron.dir";
  private static final String HERON_AURORA = "heron.aurora";

  //  private DefaultConfigLoader context;
  private LaunchContext context;
  private String dc;
  private String environ;
  private String role;
  private TopologyAPI.Topology topology;

  @Override
  public void initialize(LaunchContext context) {
    this.context = context;
    this.dc = context.getPropertyWithException(Constants.DC);
    this.environ = context.getPropertyWithException(Constants.ENVIRON);
    this.role = context.getPropertyWithException(Constants.ROLE);
    this.topology = context.getTopology();
    writeAuroraFile();
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
  public void undo() {
    // TODO(nbhagat): Delete topology if has been launched.
  }

  @Override
  public ExecutionEnvironment.ExecutionState updateExecutionState(
      ExecutionEnvironment.ExecutionState executionState) {
    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder().mergeFrom(executionState);

    builder.setDc(dc).setRole(role).setEnviron(environ);

    // Set the HeronReleaseState
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();
    releaseBuilder.setReleaseUsername(context.getPropertyWithException(Constants.HERON_RELEASE_USER_NAME));
    releaseBuilder.setReleaseTag(context.getPropertyWithException(Constants.HERON_RELEASE_TAG));
    releaseBuilder.setReleaseVersion(context.getPropertyWithException(Constants.HERON_RELEASE_VERSION));
    releaseBuilder.setUploaderVersion(context.getProperty(Constants.HERON_UPLOADER_VERSION, "live"));

    builder.setReleaseState(releaseBuilder);
    if (builder.isInitialized()) {
      return builder.build();
    } else {
      throw new RuntimeException("Failed to create execution state");
    }
  }

  private void writeAuroraFile() {
    InputStream is = this.getClass().getResourceAsStream("/heron.aurora");
    File auroraFile = new File(HERON_AURORA);
    if (auroraFile.exists()) {
      auroraFile.delete();
    }
    try (FileWriter fw = new FileWriter(auroraFile)) {
      fw.write(ShellUtility.inputstreamToString(is));
      auroraFile.deleteOnExit();
    } catch (IOException e) {
      LOG.severe("Couldn't find heron.aurora in classpath.");
      throw new RuntimeException("Couldn't find heron.aurora in classpath");
    }
  }

  private String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));
    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }


  private String getZkRoot() {
    return context.getPropertyWithException(CuratorStateManager.ZKROOT_PREFIX + "." + dc);
  }

  private String getZkHostPort() {
    String zkHost = context.getPropertyWithException(
        CuratorStateManager.ZKHOST_PREFIX + "." + dc);
    String zkPort = context.getPropertyWithException(
        CuratorStateManager.ZKPORT_PREFIX + "." + dc);
    return zkHost + ":" + zkPort;
  }

  @Override
  public boolean launchTopology(PackingPlan packing) {
    LOG.info("Launching topology in aurora");
    if (packing == null || packing.containers.isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }
    PackingPlan.Resource containerResource =
        packing.containers.values().iterator().next().resource;
    Map<String, String> auroraProperties = new HashMap<>();

    auroraProperties.put("CLASSPATH", TopologyUtility.makeClasspath(topology));
    auroraProperties.put("COMPONENT_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtility.getComponentJvmOptions(topology)));
    auroraProperties.put("COMPONENT_RAMMAP",
        TopologyUtility.formatRamMap(TopologyUtility.getComponentRamMap(topology)));
    auroraProperties.put("CPUS_PER_CONTAINER", containerResource.cpu + "");
    auroraProperties.put("DC", dc);
    auroraProperties.put("DISK_PER_CONTAINER", containerResource.disk + "");
    auroraProperties.put("ENVIRON", environ);
    auroraProperties.put("HERON_EXECUTOR_BINARY", "heron-executor");
    auroraProperties.put("HERON_INTERNALS_CONFIG_FILENAME",
        FileUtility.getBaseName(SubmitterMain.getHeronInternalsConfigFile()));
    auroraProperties.put("HERON_JAVA_HOME",
        context.getProperty("heron.java.home.path", ""));
    auroraProperties.put("INSTANCE_DISTRIBUTION", TopologyUtility.packingToString(packing));
    auroraProperties.put("INSTANCE_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtility.getInstanceJvmOptions(topology)));
    auroraProperties.put("ISPRODUCTION", "" + "prod".equals(environ));
    auroraProperties.put("JOB_NAME", topology.getName());
    auroraProperties.put("LOG_DIR",
        context.getProperty("heron.logging.directory", "log-files"));
    auroraProperties.put("HERON_PACKAGE",
        context.getPropertyWithException(Constants.HERON_RELEASE_TAG));
    auroraProperties.put("METRICS_MGR_CLASSPATH", "metrics-mgr-classpath/*");
    auroraProperties.put("NUM_SHARDS", "" + (1 + TopologyUtility.getNumContainer(topology)));
    auroraProperties.put("PKG_TYPE", (FileUtility.isOriginalPackageJar(
        FileUtility.getBaseName(SubmitterMain.getOriginalPackageFile())) ? "jar" : "tar"));
    auroraProperties.put("RAM_PER_CONTAINER", containerResource.ram + "");
    auroraProperties.put("RELEASE_ROLE",
        context.getProperty(Constants.HERON_RELEASE_USER_NAME, "heron"));
    auroraProperties.put("RUN_ROLE", role);
    auroraProperties.put("STMGR_BINARY", "heron-stmgr");
    auroraProperties.put("TMASTER_BINARY", "heron-tmaster");
    auroraProperties.put("SHELL_BINARY", "heron-shell");
    auroraProperties.put("TOPOLOGY_DEFN", topology.getName() + ".defn");
    auroraProperties.put("TOPOLOGY_ID", topology.getId());
    auroraProperties.put("TOPOLOGY_JAR_FILE",
        FileUtility.getBaseName(SubmitterMain.getOriginalPackageFile()));
    auroraProperties.put("TOPOLOGY_NAME", topology.getName());
    auroraProperties.put("TOPOLOGY_PKG",
        PackerUtility.getTopologyPackageName(topology.getName(),
            context.getProperty(Constants.HERON_RELEASE_TAG, "live")));
    auroraProperties.put("VERSION",
        context.getProperty(PackerUploader.HERON_PACKER_PKGVERSION, "live"));
    auroraProperties.put("ZK_NODE", getZkHostPort());
    auroraProperties.put("ZK_ROOT", getZkRoot());
    ArrayList<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "create", "--verbose", "--wait-until", "RUNNING"));
    for (String binding : auroraProperties.keySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", binding, auroraProperties.get(binding)));
    }
    auroraCmd.add(String.format("%s/%s/%s/%s",
        auroraProperties.get("DC"),
        auroraProperties.get("RUN_ROLE"),
        auroraProperties.get("ENVIRON"),
        auroraProperties.get("JOB_NAME")));
    auroraCmd.add(HERON_AURORA);
    return 0 == ShellUtility.runProcess(
        true, auroraCmd.toArray(new String[auroraCmd.size()]),
        new StringBuilder(), new StringBuilder());
  }

  @Override
  public boolean postLaunch(PackingPlan packing) {
    return true;
  }


}
