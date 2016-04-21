package com.twitter.heron.scheduler.reef;

import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.scheduler.SchedulerConfig;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.*;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.TopologyUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HeronExecutorTask implements Task {
  private static final Logger LOG = Logger.getLogger(HeronExecutorTask.class.getName());

  private final String topologyPackageName;
  private final String heronCorePackageName;
  private final int heronExecutorId;
  private final String cluster;
  private final String role;
  private final String topologyName;
  private final String env;
  private final String topologyJar;
  private final String packedPlan;

  private REEFFileNames reefFileNames;
  private String localHeronConfDir;

  private Config config;
  private Topology topology;

  @Inject
  public HeronExecutorTask(final REEFFileNames fileNames,
                           @Parameter(HeronExecutorId.class) String heronExecutorId,
                           @Parameter(Cluster.class) String cluster,
                           @Parameter(Role.class) String role,
                           @Parameter(TopologyName.class) String topologyName,
                           @Parameter(Environ.class) String env,
                           @Parameter(TopologyPackageName.class) String topologyPackageName,
                           @Parameter(HeronCorePackageName.class) String heronCorePackageName,
                           @Parameter(TopologyJar.class) String topologyJar,
                           @Parameter(PackedPlan.class) String packedPlan) {
    this.heronExecutorId = Integer.valueOf(heronExecutorId);
    this.cluster = cluster;
    this.role = role;
    this.topologyName = topologyName;
    this.topologyPackageName = topologyPackageName;
    this.heronCorePackageName = heronCorePackageName;
    this.env = env;
    this.topologyJar = topologyJar;
    this.packedPlan = packedPlan;

    reefFileNames = fileNames;
    localHeronConfDir = ".";
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    String globalFolder = reefFileNames.getGlobalFolder().getPath();

    extractPackageInSandbox(globalFolder, topologyPackageName, localHeronConfDir);
    extractPackageInSandbox(globalFolder, heronCorePackageName, localHeronConfDir);

    String topologyDefnFile = TopologyUtils.lookUpTopologyDefnFile(".", topologyName);
    topology = TopologyUtils.getTopology(topologyDefnFile);
    config = new ConfigLoader().getConfig(cluster, role, env, topologyJar, topologyDefnFile, topology);

    LOG.log(Level.INFO, "Preparing evaluator for running executor-id: {0}", heronExecutorId);

    String executorCommand = getExecutorCommand(heronExecutorId);

    final Process regularExecutor = ShellUtils.runASyncProcess(true, executorCommand, new File("."));
    LOG.log(Level.INFO, "Started heron executor-id: {0}", heronExecutorId);
    regularExecutor.waitFor();
    return null;
  }

  private String getExecutorCommand(int container) {
    int port1 = NetworkUtils.getFreePort();
    int port2 = NetworkUtils.getFreePort();
    int port3 = NetworkUtils.getFreePort();
    int shellPort = NetworkUtils.getFreePort();
    int port4 = NetworkUtils.getFreePort();

    if (port1 == -1 || port2 == -1 || port3 == -1) {
      throw new RuntimeException("Could not find available ports to start topology");
    }

    String executorCmd = String.format(
            "%s %d %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %d %s %s %d %s %s %s %s %s %s %d",
            Context.executorSandboxBinary(config),
            container,
            topology.getName(),
            topology.getId(),
            FilenameUtils.getName(Context.topologyDefinitionFile(config)),
            packedPlan,
            Context.stateManagerConnectionString(config),
            Context.stateManagerRootPath(config),
            Context.tmasterSandboxBinary(config),
            Context.stmgrSandboxBinary(config),
            Context.metricsManagerSandboxClassPath(config),
            formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)),
            TopologyUtils.makeClassPath(topology, Context.topologyJarFile(config)),
            port1,
            port2,
            port3,
            Context.systemConfigSandboxFile(config),
            TopologyUtils.formatRamMap(TopologyUtils.getComponentRamMap(topology, Context.instanceRam(config))),
            formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)),
            Context.topologyPackageType(config),
            Context.topologyJarFile(config),
            Context.javaSandboxHome(config),
            shellPort,
            Context.logSandboxDirectory(config),
            Context.shellSandboxBinary(config),
            port4,
            Context.cluster(config),
            Context.role(config),
            Context.environ(config),
            Context.instanceSandboxClassPath(config),
            Context.metricsSinksSandboxFile(config),
            "no_need_since_scheduler_is_started",
            0);

    LOG.log(Level.INFO, "Executor command line: {0}", executorCmd);

    return executorCmd;
  }

  private void extractPackageInSandbox(String srcFolder, String fileName, String dstDir) {
    String packagePath = Paths.get(srcFolder, fileName).toString();
    LOG.log(Level.INFO, "Extracting package: {0} at: {1}", new Object[]{packagePath, dstDir});
    boolean result = untarPackage(packagePath, dstDir);
    if (!result) {
      String msg = "Failed to extract package:" + packagePath + " at: " + dstDir;
      LOG.log(Level.SEVERE, msg);
      throw new RuntimeException(msg);
    }
  }

  /**
   * TODO copied from localScheduler. May be moved to a utils class
   */
  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(javaOpts.getBytes(Charset.forName("UTF-8")));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  /**
   * TODO this method from LocalLauncher could be moved to a utils class
   */
  protected boolean untarPackage(String packageName, String targetFolder) {
    String cmd = String.format("tar -xvf %s", packageName);

    int ret = ShellUtils.runSyncProcess(false,
            true,
            cmd,
            new StringBuilder(),
            new StringBuilder(),
            new File(targetFolder));

    return ret == 0 ? true : false;
  }

  /*
   * TODO This class could be removed when a util class is created
   */
  private class ConfigLoader extends SchedulerConfig {
    public Config getConfig(String cluster, String role, String env, String jar, String defn, Topology topology) {
      return super.loadConfig(cluster, role, env, jar, defn, topology);
    }
  }
}
