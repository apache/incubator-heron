package com.twitter.heron.scheduler.aurora;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * Launch topology locally to Aurora.
 */

public class AuroraLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  private Config config;
  private Config runtime;

  @Override
  public void initialize(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;
  }

  @Override
  public boolean prepareLaunch(PackingPlan packing) {
    return true;
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

  @Override
  public boolean launch(PackingPlan packing) {
    LOG.info("Launching topology in aurora");

    TopologyAPI.Topology topology = Runtime.topology(runtime);

    if (packing == null || packing.containers.isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }
    PackingPlan.Resource containerResource =
        packing.containers.values().iterator().next().resource;
    Map<String, String> auroraProperties = new HashMap<>();

    auroraProperties.put("SANDBOX_EXECUTOR_BINARY", Context.executorSandboxBinary(config));
    auroraProperties.put("TOPOLOGY_NAME", topology.getName());
    auroraProperties.put("TOPOLOGY_ID", topology.getId());
    auroraProperties.put("TOPOLOGY_DEFINITION_FILE", FileUtils.getBaseName(Context.topologyDefinitionFile(config)));
    auroraProperties.put("INSTANCE_DISTRIBUTION", TopologyUtils.packingToString(packing));
    auroraProperties.put("STATEMGR_CONNECTION_STRING", Context.stateManagerConnectionString(config));
    auroraProperties.put("STATEMGR_ROOT_PATH", Context.stateManagerRootPath(config));
    auroraProperties.put("SANDBOX_TMASTER_BINARY", Context.tmasterSandboxBinary(config));
    auroraProperties.put("SANDBOX_STMGR_BINARY", Context.stmgrSandboxBinary(config));
    auroraProperties.put("SANDBOX_METRICSMGR_CLASSPATH", Context.metricsManagerSandboxClassPath(config));
    auroraProperties.put("INSTANCE_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    auroraProperties.put("TOPOLOGY_CLASSPATH", TopologyUtils.makeClassPath(topology, Context.topologyJarFile(config)));

    auroraProperties.put("SANDBOX_SYSTEM_YAML", Context.systemConfigSandboxFile(config));
    auroraProperties.put("COMPONENT_RAMMAP",
        TopologyUtils.formatRamMap(
            TopologyUtils.getComponentRamMap(topology, Context.instanceRam(config))));
    auroraProperties.put("COMPONENT_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    auroraProperties.put("TOPOLOGY_PACKAGE_TYPE", Context.topologyPackageType(config));
    auroraProperties.put("TOPOLOGY_JAR_FILE",
        FileUtils.getBaseName(Context.topologyJarFile(config)));
    auroraProperties.put("HERON_SANDBOX_JAVA_HOME", Context.javaSandboxHome(config));

    auroraProperties.put("SANDBOX_LOGGING_DIRECTORY", Context.logSandboxDirectory(config));
    auroraProperties.put("SANDBOX_SHELL_BINARY", Context.shellSandboxBinary(config));

    auroraProperties.put("CPUS_PER_CONTAINER", containerResource.cpu + "");
    auroraProperties.put("DISK_PER_CONTAINER", containerResource.disk + "");
    auroraProperties.put("RAM_PER_CONTAINER", containerResource.ram + "");

    auroraProperties.put("NUM_CONTAINERS", (1 + TopologyUtils.getNumContainers(topology)) + "");

    auroraProperties.put("CLUSTER", Context.cluster(config));
    auroraProperties.put("ENVIRON", Context.environ(config));
    auroraProperties.put("ROLE", Context.role(config));

    auroraProperties.put("SANDBOX_INSTANCE_CLASSPATH", Context.instanceSandboxClassPath(config));
    auroraProperties.put("SANDBOX_METRICS_YAML", Context.metricsSinksSandboxFile(config));
    auroraProperties.put("SANDBOX_SCHEDULER_CLASSPATH", Context.schedulerSandboxClassPath(config));

    // TODO(mfu): Following configs need customization before using
    // TODO(mfu): Put the constant in Constants.java
    String heronCoreReleasePkgURI = Context.corePackageUri(config);
    String topologyPkgURI = Runtime.topologyPackageUri(runtime);

    auroraProperties.put("CORE_PACKAGE_URI", heronCoreReleasePkgURI);
    auroraProperties.put("TOPOLOGY_PACKAGE_URI", topologyPkgURI);

    return AuroraUtils.createAuroraJob(topology.getName(), Context.cluster(config),
        Context.role(config),
        Context.environ(config), getHeronAuroraPath(), auroraProperties, true);
  }

  private String getHeronAuroraPath() {
    return new File(Context.heronConf(config), "heron.aurora").getPath();
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
  public ExecutionEnvironment.ExecutionState updateExecutionState(ExecutionEnvironment.ExecutionState executionState) {
    // TODO(mfu): These values should read from config
    String releaseUsername = "heron";
    String releaseTag = "heron-core-release";
    String releaseVersion = "releaseVersion";
    String uploadVersion = "uploadVersion";

    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder().mergeFrom(executionState);

    builder.setCluster(Context.cluster(config))
        .setRole(Context.role(config))
        .setEnviron(Context.environ(config));

    // Set the HeronReleaseState
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();

    releaseBuilder.setReleaseUsername(releaseUsername);
    releaseBuilder.setReleaseTag(releaseTag);
    releaseBuilder.setReleaseVersion(releaseVersion);
    releaseBuilder.setUploaderVersion(uploadVersion);

    builder.setReleaseState(releaseBuilder);
    if (builder.isInitialized()) {
      return builder.build();
    } else {
      throw new RuntimeException("Failed to create execution state");
    }
  }
}
