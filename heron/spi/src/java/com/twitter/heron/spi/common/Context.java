package com.twitter.heron.spi.common;

public class Context {

  public static String cluster(Config cfg) {
    return cfg.getStringValue(Keys.get("CLUSTER"));
  }

  public static String role(Config cfg) {
    return cfg.getStringValue(Keys.get("ROLE"));
  }

  public static String environ(Config cfg) {
    return cfg.getStringValue(Keys.get("ENVIRON"));
  }

  public static Boolean verbose(Config cfg) {
    return cfg.getBooleanValue(Keys.get("VERBOSE"));
  }

  public static String configPath(Config cfg) {
    return cfg.getStringValue(Keys.get("CONFIG_PATH"));
  }

  public static String topologyName(Config cfg) {
    return cfg.getStringValue(Keys.get("TOPOLOGY_NAME"));
  }

  public static String uploaderClass(Config cfg) {
    return cfg.getStringValue(Keys.get("UPLOADER_CLASS"));
  }

  public static String launcherClass(Config cfg) {
    return cfg.getStringValue(Keys.get("LAUNCHER_CLASS"));
  }

  public static String schedulerClass(Config cfg) {
    return cfg.getStringValue(Keys.get("SCHEDULER_CLASS"));
  }

  public static String runtimeManagerClass(Config cfg) {
    return cfg.getStringValue(Keys.get("RUNTIME_MANAGER_CLASS"));
  }

  public static String packingClass(Config cfg) {
    return cfg.getStringValue(Keys.get("PACKING_CLASS"));
  }

  public static String stateManagerClass(Config cfg) {
    return cfg.getStringValue(Keys.get("STATE_MANAGER_CLASS"));
  }

  public static String executorBinary(Config cfg) {
    return cfg.getStringValue(Keys.get("EXECUTOR_BINARY"));
  }

  public static String stmgrBinary(Config cfg) {
    return cfg.getStringValue(Keys.get("STMGR_BINARY"));
  }

  public static String tmasterBinary(Config cfg) {
    return cfg.getStringValue(Keys.get("TMASTER_BINARY"));
  }

  public static String shellBinary(Config cfg) {
    return cfg.getStringValue(Keys.get("SHELL_BINARY"));
  }

  public static String schedulerJar(Config cfg) {
    return cfg.getStringValue(Keys.get("SCHEDULER_JAR"));
  }

  public static String stateManagerConnectionString(Config cfg) {
    return cfg.getStringValue(Keys.get("STATEMGR_CONNECTION_STRING"));
  }

  public static String stateManagerRootPath(Config cfg) {
    return cfg.getStringValue(Keys.get("STATEMGR_ROOT_PATH"));
  }

  public static String corePackageUri(Config cfg) {
    return cfg.getStringValue(Keys.get("CORE_PACKAGE_URI"));
  }

  public static String logDirectory(Config cfg) {
    return cfg.getStringValue(Keys.get("LOGGING_DIRECTORY"));
  }
 
  public static String systemConfigFile(Config cfg) {
    return cfg.getStringValue(Keys.get("SYSTEM_YAML"));
  }

  public static String topologyDefinitionFile(Config cfg) {
    return cfg.getStringValue(Keys.get("TOPOLOGY_DEFINITION_FILE"));
  }

  public static String topologyJarFile(Config cfg) {
    return cfg.getStringValue(Keys.get("TOPOLOGY_JAR_FILE"));
  }

  public static String topologyPackageFile(Config cfg) {
    return cfg.getStringValue(Keys.get("TOPOLOGY_PACKAGE_FILE"));
  }

  public static String topologyPackageType(Config cfg) {
    return cfg.getStringValue(Keys.get("TOPOLOGY_PACKAGE_TYPE"));
  }

  // Constants for config provided default values for resources
  public static Long stmgrRam(Config cfg) {
    return cfg.getLongValue(Keys.get("STMGR_RAM"));
  }

  public static Long instanceRam(Config cfg) {
    return cfg.getLongValue(Keys.get("INSTANCE_RAM"));
  }

  public static Double instanceCpu(Config cfg) {
    return cfg.getDoubleValue(Keys.get("INSTANCE_CPU"));
  }

  public static Long instanceDisk(Config cfg) {
    return cfg.getLongValue(Keys.get("INSTANCE_DISK"));
  }

  public static String heronHome(Config cfg) {
    return cfg.getStringValue(Keys.get("HERON_HOME")); 
  }

  public static String heronBin(Config cfg) {
    return cfg.getStringValue(Keys.get("HERON_BIN")); 
  }
  
  public static String heronConf(Config cfg) {
    return cfg.getStringValue(Keys.get("HERON_CONF"));
  }

  public static final String heronLib(Config cfg) {
    return cfg.getStringValue(Keys.get("HERON_LIB"));
  }

  public static final String heronDist(Config cfg) {
    return cfg.getStringValue(Keys.get("HERON_DIST"));
  }

  public static final String heronEtc(Config cfg) {
    return cfg.getStringValue(Keys.get("HERON_ETC"));
  }

  public static final String javaHome(Config cfg) {
    String javaHome = cfg.getStringValue(Keys.get("JAVA_HOME"));
    return javaHome == null ? System.getenv("JAVA_HOME") : javaHome;
  }

  public static final Boolean isZkStateManager(Config cfg) {
    String stateManagerClass = cfg.getStringValue(Keys.get("STATE_MANAGER_CLASS"));
    return stateManagerClass.equals(Constants.ZK_STATE_MANAGER_CLASS) ? true : false;
  }
}
