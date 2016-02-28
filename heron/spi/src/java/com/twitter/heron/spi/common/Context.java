package com.twitter.heron.spi.common;

public class Context {

  public static String cluster(Config cfg) {
    return cfg.getStringValue(Keys.CLUSTER);
  }

  public static String role(Config cfg) {
    return cfg.getStringValue(Keys.ROLE);
  }

  public static String environ(Config cfg) {
    return cfg.getStringValue(Keys.ENVIRON);
  }

  public static Boolean verbose(Config cfg) {
    return cfg.getBooleanValue(Keys.VERBOSE);
  }

  public static String configPath(Config cfg) {
    return cfg.getStringValue(Keys.CONFIG_PATH);
  }

  public static String topologyName(Config cfg) {
    return cfg.getStringValue(Keys.TOPOLOGY_NAME);
  }

  public static String uploaderClass(Config cfg) {
    return cfg.getStringValue(Keys.UPLOADER_CLASS);
  }

  public static String launcherClass(Config cfg) {
    return cfg.getStringValue(Keys.LAUNCHER_CLASS);
  }

  public static String schedulerClass(Config cfg) {
    return cfg.getStringValue(Keys.SCHEDULER_CLASS);
  }

  public static String runtimeManagerClass(Config cfg) {
    return cfg.getStringValue(Keys.RUNTIME_MANAGER_CLASS);
  }

  public static String packingClass(Config cfg) {
    return cfg.getStringValue(Keys.PACKING_CLASS);
  }

  public static String stateManagerClass(Config cfg) {
    return cfg.getStringValue(Keys.STATE_MANAGER_CLASS);
  }

  public static String executorBinary(Config cfg) {
    return cfg.getStringValue(Keys.EXECUTOR_BINARY);
  }

  public static String stmgrBinary(Config cfg) {
    return cfg.getStringValue(Keys.STMGR_BINARY);
  }

  public static String tmasterBinary(Config cfg) {
    return cfg.getStringValue(Keys.TMASTER_BINARY);
  }

  public static String shellBinary(Config cfg) {
    return cfg.getStringValue(Keys.SHELL_BINARY);
  }

  public static String schedulerJar(Config cfg) {
    return cfg.getStringValue(Keys.SCHEDULER_JAR);
  }

  public static String stateRootPath(Config cfg) {
    return cfg.getStringValue(Keys.STATE_ROOT_PATH);
  }

  public static String logDirectory(Config cfg) {
    return cfg.getStringValue(Keys.LOGGING_DIRECTORY);
  }
 
  public static String internalsConfigFile(Config cfg) {
    return cfg.getStringValue(Keys.INTERNALS_CONFIG_FILE);
  }

  public static String topologyDefinitionFile(Config cfg) {
    return cfg.getStringValue(Keys.TOPOLOGY_DEFINITION_FILE);
  }

  public static String topologyJarFile(Config cfg) {
    return cfg.getStringValue(Keys.TOPOLOGY_JAR_FILE);
  }

  public static String topologyPackageUri(Config cfg) {
    return cfg.getStringValue(Keys.TOPOLOGY_PACKAGE_URI);
  }

  public static String topologyPackageFile(Config cfg) {
    return cfg.getStringValue(Keys.TOPOLOGY_PACKAGE_FILE);
  }

  public static String topologyPackageType(Config cfg) {
    return cfg.getStringValue(Keys.TOPOLOGY_PACKAGE_TYPE);
  }

  // Constants for config provided default values for resources
  public static Long stmgrRam(Config cfg) {
    return cfg.getLongValue(Keys.STMGR_RAM);
  }

  public static Long instanceRam(Config cfg) {
    return cfg.getLongValue(Keys.INSTANCE_RAM);
  }

  public static Double instanceCpu(Config cfg) {
    return cfg.getDoubleValue(Keys.INSTANCE_CPU);
  }

  public static Long instanceDisk(Config cfg) {
    return cfg.getLongValue(Keys.INSTANCE_DISK);
  }

  public static String heronHome(Config cfg) {
    return cfg.getStringValue(Keys.HERON_HOME); 
  }

  public static String heronBin(Config cfg) {
    return cfg.getStringValue(Keys.HERON_BIN); 
  }
  
  public static String heronConf(Config cfg) {
    return cfg.getStringValue(Keys.HERON_CONF);
  }

  public static final String heronLib(Config cfg) {
    return cfg.getStringValue(Keys.HERON_LIB);
  }

  public static final String heronDist(Config cfg) {
    return cfg.getStringValue(Keys.HERON_DIST);
  }

  public static final String heronEtc(Config cfg) {
    return cfg.getStringValue(Keys.HERON_ETC);
  }
}
