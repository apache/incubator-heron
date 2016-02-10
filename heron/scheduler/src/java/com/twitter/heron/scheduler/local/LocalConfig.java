package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.Context;
import com.twitter.heron.state.FileSystemStateManager;

public class LocalConfig {
  private final Context properties;

  public LocalConfig(Context context) {
    this.properties = context;
  }

  public static final String WORKING_DIRECTORY = "heron.local.working.directory";
  public static final String LOG_DIR = "heron.logging.directory";

  public static final String CLASS_PATH = "heron.class.path";
  public static final String COMPONENT_JVM_OPTS_IN_BASE64 = "heron.component.jvm.opts.in.base64";
  public static final String COMPONENT_RAMMAP = "heron.component.rammap";
  public static final String HERON_EXECUTOR_BINARY = "heron.executor.binary";
  public static final String HERON_SCHEDULER_BINARY = "heron.scheduler.binary";
  public static final String HERON_INTERNALS_CONFIG_FILENAME = "heron.internals.conf.filename";
  public static final String HERON_JAVA_HOME = "heron.java.home.path";
  public static final String INSTANCE_DISTRIBUTION = "heron.instance.distribution";
  public static final String INSTANCE_JVM_OPTS_IN_BASE64 = "heron.instance.jvm.opts.in.base64";
  public static final String METRICS_MGR_CLASSPATH = "heron.metrics.mgr.classpath";
  public static final String NUM_SHARDS = "heron.num.shards";
  public static final String PKG_TYPE = "heron.pkg.type";
  public static final String STMGR_BINARY = "heron.stmgr.binary";
  public static final String TMASTER_BINARY = "heron.tmaster.binary";
  public static final String HERON_SHELL_BINARY = "heron.shell.binary";

  public static final String TOPOLOGY_ID = "heron.topology.id";
  public static final String TOPOLOGY_JAR_FILE = "heron.topology.jar.file";
  public static final String TOPOLOGY_NAME = "heron.topology.name";

  public static final String HERON_CORE_RELEASE_PACKAGE = "heron.core.release.package";


  public String getWorkingDirectory() {
    return properties.getProperty(WORKING_DIRECTORY);
  }

  public String getLogDir() {
    return properties.getProperty(LOG_DIR);
  }

  public String getClasspath() {
    return properties.getProperty(CLASS_PATH);
  }

  public String getComponentJVMOptionsBase64() {
    return properties.getProperty(COMPONENT_JVM_OPTS_IN_BASE64);
  }

  public String getComponentRamMap() {
    return properties.getProperty(COMPONENT_RAMMAP);
  }

  public String getExecutorBinary() {
    return properties.getProperty(HERON_EXECUTOR_BINARY);
  }

  public String getHeronSchedulerBinary() {
    return properties.getProperty(HERON_SCHEDULER_BINARY);
  }

  public String getHeronShellBinary() {
    return properties.getProperty(HERON_SHELL_BINARY);
  }

  public String getInternalConfigFilename() {
    return properties.getProperty(HERON_INTERNALS_CONFIG_FILENAME);
  }

  public String getJavaHome() {
    return properties.getProperty(HERON_JAVA_HOME);
  }

  public String getInstanceDistribution() {
    return properties.getProperty(INSTANCE_DISTRIBUTION);
  }

  public String getInstanceJvmOptionsBase64() {
    return properties.getProperty(INSTANCE_JVM_OPTS_IN_BASE64);
  }

  public String getMetricsMgrClasspath() {
    return properties.getProperty(METRICS_MGR_CLASSPATH);
  }

  public String getNumShard() {
    return properties.getProperty(NUM_SHARDS);
  }

  public String getPkgType() {
    return properties.getProperty(PKG_TYPE);
  }

  public String getStmgrBinary() {
    return properties.getProperty(STMGR_BINARY);
  }

  public String getTMasterBinary() {
    return properties.getProperty(TMASTER_BINARY);
  }

  public String getTopologyDef() {
    return properties.getProperty(Constants.TOPOLOGY_DEFINITION_FILE);
  }

  public String getTopologyId() {
    return properties.getProperty(TOPOLOGY_ID);
  }

  public String getTopologyJarFile() {
    return properties.getProperty(TOPOLOGY_JAR_FILE);
  }

  public String getTopologyName() {
    return properties.getProperty(TOPOLOGY_NAME);
  }

  public String getZkRoot() {
    // Use working directory if zk is not to be used
    return properties.getPropertyWithException(FileSystemStateManager.ROOT_ADDRESS);
  }

  public String getZkNode() {
    // Use constants "LOCALMODE" when if zk is not to be used
    return isZkUsedForStateManager() ?
        properties.getPropertyWithException(Constants.ZK_CONNECTION_STRING) :
        "LOCALMODE";
  }

  public boolean isZkUsedForStateManager() {
    return properties.getProperty(Constants.ZK_CONNECTION_STRING) != null;
  }

  public String getTopologyPackage() {
    return "topology.tar.gz";
  }

  public String getHeronCoreReleasePackage() {
    return properties.getProperty(HERON_CORE_RELEASE_PACKAGE);
  }
}
