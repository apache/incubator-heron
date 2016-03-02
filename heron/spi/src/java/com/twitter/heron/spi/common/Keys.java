package com.twitter.heron.spi.common;

public class Keys {

  // Constants for config provided in the command line
  public static final String CLUSTER = "heron.config.cluster";
  public static final String ROLE = "heron.config.role";
  public static final String ENVIRON = "heron.config.environ";
  public static final String VERBOSE = "heron.config.verbose";
  public static final String CONFIG_PATH = "heron.config.path";

  // Constants for config provided user classes
  public static final String UPLOADER_CLASS = "heron.class.uploader";
  public static final String LAUNCHER_CLASS = "heron.class.launcher";
  public static final String SCHEDULER_CLASS = "heron.class.scheduler";
  public static final String RUNTIME_MANAGER_CLASS = "heron.class.runtime.manager";
  public static final String PACKING_CLASS = "heron.class.packing.algorithm";
  public static final String STATE_MANAGER_CLASS = "heron.class.state.manager";

  // Constants for config provided user binaries
  public static final String EXECUTOR_BINARY = "heron.binaries.executor";
  public static final String STMGR_BINARY = "heron.binaries.stmgr";
  public static final String TMASTER_BINARY = "heron.binaries.tmaster";
  public static final String SHELL_BINARY = "heron.binaries.shell";
  public static final String SCHEDULER_JAR = "heron.jars.scheduler";

  // Constants for config provided files and directories
  public static final String LOGGING_DIRECTORY = "heron.directory.logging";
  public static final String INTERNALS_CONFIG_FILE = "heron.internals.config.file";

  // Constants for packages URIs
  public static final String CORE_PACKAGE_URI = "heron.package.core.uri";
  public static final String TOPOLOGY_PACKAGE_URI = "heron.package.topology.uri";

  // Constants for topology 
  public static final String TOPOLOGY_ID = "heron.topology.id";
  public static final String TOPOLOGY_NAME = "heron.topology.name";
  public static final String TOPOLOGY_DEFINITION_FILE = "heron.topology.definition.file";
  public static final String TOPOLOGY_DEFINITION = "heron.topology.definition";
  public static final String TOPOLOGY_JAR_FILE = "heron.topology.jar.file";
  public static final String TOPOLOGY_PACKAGE_FILE = "heron.topology.package.file";
  public static final String TOPOLOGY_PACKAGE_TYPE = "heron.topology.package.type";

  // Constants for storing state
  public static final String STATEMGR_CONNECTION_STRING = "heron.statemgr.connection.string";
  public static final String STATEMGR_ROOT_PATH = "heron.statemgr.root.path";

  // Constants for config provided default values for resources
  public static final String STMGR_RAM = "heron.resources.stmgr.ram";
  public static final String INSTANCE_RAM = "heron.resources.instance.ram";
  public static final String INSTANCE_CPU = "heron.resources.instance.cpu";
  public static final String INSTANCE_DISK = "heron.resources.instance.disk";

  // Constants for config provided paths
  public static final String METRICS_MANAGER_CLASSPATH = "heron.metrics.manager.classpath";

  // Constants for heron environment
  public static final String HERON_HOME = "heron.directory.home";
  public static final String HERON_BIN = "heron.directory.bin"; 
  public static final String HERON_CONF = "heron.directory.conf";
  public static final String HERON_LIB = "heron.directory.lib";
  public static final String HERON_DIST = "heron.directory.dist";
  public static final String HERON_ETC  = "heron.directory.etc";

  // Constants for heron configuration files
  public static final String CLUSTER_YAML = "heron.config.file.cluster.yaml";
  public static final String DEFAULTS_YAML = "heron.config.file.defaults.yaml";
  public static final String METRICS_YAML = "heron.config.file.metrics.yaml";
  public static final String PACKING_YAML = "heron.config.file.packing.yaml";
  public static final String SCHEDULER_YAML = "heron.config.file.scheduler.yaml";
  public static final String STATEMGR_YAML = "heron.config.file.statemgr.yaml";
  public static final String SYSTEM_YAML = "heron.config.file.system.yaml";
  public static final String UPLOADER_YAML = "heron.config.file.uploader.yaml";

  // Constants for run time config
  public static final String TOPOLOGY_CLASS_PATH = "heron.runtime.topology.class.path";
  public static final String STATE_MANAGER = "heron.runtime.state.manager";
  public static final String JAVA_HOME = "heron.runtime.java.home.path";

  public static final String COMPONENT_RAMMAP = "heron.runtime.component.rammap";
  public static final String COMPONENT_JVM_OPTS_IN_BASE64 = "heron.runtime.component.jvm.opts.in.base64";
  public static final String INSTANCE_DISTRIBUTION = "heron.runtime.instance.distribution";
  public static final String INSTANCE_JVM_OPTS_IN_BASE64 = "heron.runtime.instance.jvm.opts.in.base64";

  public static final String METRICS_MGR_CLASSPATH = "heron.runtime.metrics.mgr.classpath";
  public static final String NUM_CONTAINERS = "heron.runtime.num.containers";

  // Rest 
  public static final String HERON_RELEASE_PACKAGE = "heron.release.package";
  public static final String HERON_RELEASE_PACKAGE_ROLE = "heron.release.package.role";
  public static final String HERON_RELEASE_PACKAGE_NAME = "heron.release.package.name";
  public static final String HERON_RELEASE_PACKAGE_VERSION = "heron.release.package.version";
  public static final String HERON_UPLOADER_VERSION = "heron.uploader.version";
  
  public static final String HERON_AURORA_BIND_PREFIX = "heron.aurora.bind.";
  public static final String HERON_VERBOSE = "heron.verbose";
  public static final String CONFIG_PROPERTY = "config.property";
}
