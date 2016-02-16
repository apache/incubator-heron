package com.twitter.heron.spi.common;

public class Defaults {

  protected static final long GB = 1024L * 1024 * 1024;
  protected static final long MB = 1024L * 1024;

  /**
   *  Default values for config related directories and files
   */
  public class Files {
    public static final String CLUSTER_YAML = "cluster.yaml";
    public static final String DEFAULTS_YAML = "defaults.yaml";
    public static final String PACKING_YAML = "packing.yaml";
    public static final String SCHEDULER_YAML = "scheduler.yaml";
    public static final String STATEMGR_YAML = "statemgr.yaml";
    public static final String UPLOADER_YAML = "uploader.yaml";

    public static final String LOGGING_DIRECTORY = "log-files";
  }

  /**
   *  Default values for config binaries, jars and resource values 
   */
  public class Config { 
    // Default values for config provided user binaries
    public static final String EXECUTOR_BINARY = "heron-executor";
    public static final String STMGR_BINARY = "heron-stmgr";
    public static final String TMASTER_BINARY = "heron-tmaster";
    public static final String SHELL_BINARY = "heron-shell";
    public static final String SCHEDULER_JAR = "heron-scheduler.jar";

    // Defaults for config provide paths
    public static final String METRICS_MANAGER_CLASSPATH = "metrics-mgr-classpath/*";

    // Constants for config provided default values for resources
    public static final long STMGR_RAM = 128 * MB;
    public static final long INSTANCE_CPU = 1;
    public static final long INSTANCE_RAM = 128 * MB;
    public static final long INSTANCE_DISK = 128 * MB;
  }
}
