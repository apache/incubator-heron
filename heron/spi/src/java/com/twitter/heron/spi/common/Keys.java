package com.twitter.heron.spi.common;

public class Keys {
  public class Metrics {
    public static final long GB = 1024L * 1024 * 1024;
    public static final long MB = 1024L * 1024;
  }

  public static final String HERON_CORE_RELEASE_URI = "heron.core.release.uri";
  public static final String TOPOLOGY_PKG_URI = "heron.topology.pkg.uri";

  public class Config { 
    // Constants for config provided in the command line
    public static final String CLUSTER = "heron.config.cluster";
    public static final String ROLE = "heron.config.role";
    public static final String ENVIRON = "heron.config.environ";
    public static final String VERBOSE = "heron.config.verbose";
    public static final String CONFIG_PATH = "heron.config.path";
    public static final String TOPOLOGY_NAME = "heron.config.topology.name";

    // Constants for config provided user classes
    public static final String UPLOADER_CLASS = "heron.uploader.class";
    public static final String LAUNCHER_CLASS = "heron.launcher.class";
    public static final String SCHEDULER_CLASS = "heron.scheduler.class";
    public static final String RUNTIME_MANAGER_CLASS = "heron.runtime.manager.class";
    public static final String PACKING_CLASS = "heron.packing.algorithm.class";
    public static final String STATE_MANAGER_CLASS = "heron.state.manager.class";

    // Constants for config provided user binaries
    public static final String EXECUTOR_BINARY = "heron.binaries.executor";
    public static final String STMGR_BINARY = "heron.binaries.stmgr";
    public static final String TMASTER_BINARY = "heron.binaries.tmaster";
    public static final String SHELL_BINARY = "heron.binaries.shell";
    public static final String SCHEDULER_JAR = "heron.jars.scheduler";

    // Constants for config provided files and directories
    public static final String LOGGING_DIRECTORY = "heron.logging.directory";
    public static final String INTERNALS_CONFIG_FILE = "heron.internals.config.file";
    public static final String TOPOLOGY_DEFINITION_FILE = "heron.topology.definition.file";
    public static final String TOPOLOGY_JAR_FILE = "heron.topology.jar.file";
    public static final String TOPOLOGY_PKG_TYPE = "heron.topology.package.type";

    // Constants for config provided default values for resources
    public static final String STMGR_RAM = "heron.stmgr.ram";
    public static final String INSTANCE_RAM = "heron.instance.ram";
    public static final String INSTANCE_CPU = "heron.instance.cpu";
    public static final String INSTANCE_DISK = "heron.instance.disk";

    // Consts for config provided paths
    public static final String METRICS_MANAGER_CLASSPATH = "heron.metrics.manager.classpath";
  }

  public class Runtime {
    public static final String CORE_RELEASE_URI = "heron.core.release.uri";
    public static final String TOPOLOGY_PKG_URI = "heron.topology.pkg.uri";
    public static final String TOPOLOGY_PHYSICAL_PLAN = "heron.topology.physical.plan";

    public static final String HERON_RELEASE_PACKAGE = "heron.release.package";
    public static final String HERON_RELEASE_PACKAGE_ROLE = "heron.release.package.role";
    public static final String HERON_RELEASE_PACKAGE_NAME = "heron.release.package.name";
    public static final String HERON_RELEASE_PACKAGE_VERSION = "heron.release.package.version";
    public static final String HERON_UPLOADER_VERSION = "heron.uploader.version";
 }
  

  public static final String HERON_DIR = "heron.dir";
  public static final String HERON_CONFIG_LOADER = "heron.config.loader";
  public static final String HERON_AURORA_BIND_PREFIX = "heron.aurora.bind.";
  public static final String HERON_VERBOSE = "heron.verbose";
  public static final String CONFIG_PROPERTY = "config.property";
}
