package com.twitter.heron.spi.common;

public class Defaults {

  /**
   *  Default values for config related directories and files
   */
  public static final String CLUSTER_YAML       = "${HERON_CONF}/cluster.yaml";
  public static final String DEFAULTS_YAML      = "${HERON_CONF}/defaults.yaml";
  public static final String METRICS_YAML       = "${HERON_CONF}/metrics_sinks.yaml";
  public static final String PACKING_YAML       = "${HERON_CONF}/packing.yaml";
  public static final String SCHEDULER_YAML     = "${HERON_CONF}/scheduler.yaml";
  public static final String STATEMGR_YAML      = "${HERON_CONF}/statemgr.yaml";
  public static final String SYSTEM_YAML        = "${HERON_CONF}/heron_internals.yaml";
  public static final String UPLOADER_YAML      = "${HERON_CONF}/uploader.yaml";

  public static final String LOGGING_DIRECTORY  = "./log-files";

  /**
   *  Default values for config binaries, jars and resource values 
   */
  // Default values for config provided user binaries
  public static final String EXECUTOR_BINARY    = "${HERON_BIN}/heron-executor";
  public static final String STMGR_BINARY       = "${HERON_BIN}/heron-stmgr";
  public static final String TMASTER_BINARY     = "${HERON_BIN}/heron-tmaster";
  public static final String SHELL_BINARY       = "${HERON_BIN}/heron-shell";
  public static final String SCHEDULER_JAR      = "${HERON_LIB}/heron-scheduler.jar";

  // Defaults for config provide files, paths and directories
  public static final String CORE_PACKAGE_URI   = "${HERON_DIST}/heron-core.tar.gz";
  public static final String METRICS_MANAGER_CLASSPATH = "metrics-mgr-classpath/*";

  // Constants for config provided default values for resources
  public static final long STMGR_RAM            = 1 * Constants.GB;
  public static final long INSTANCE_CPU         = 1;
  public static final long INSTANCE_RAM         = 1 * Constants.GB;
  public static final long INSTANCE_DISK        = 1 * Constants.GB;

  public static final String HERON_HOME         = "/usr/local/heron";
  public static final String HERON_CONF         = "${HERON_HOME}/conf";
  public static final String HERON_BIN          = "${HERON_HOME}/bin";
  public static final String HERON_DIST         = "${HERON_HOME}/dist";
  public static final String HERON_ETC          = "${HERON_HOME}/etc";
  public static final String HERON_LIB          = "${HERON_HOME}/lib";

  public static final String HERON_SANDBOX_HOME = "./heron-core";
  public static final String HERON_SANDBOX_CONF = "./heron-conf";
}
