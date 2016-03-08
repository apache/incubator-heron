package com.twitter.heron.spi.common;

public class Context {

  public static String cluster(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("CLUSTER"));
  }

  public static String role(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("ROLE"));
  }

  public static String environ(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("ENVIRON"));
  }

  public static Boolean verbose(Config cfg) {
    return cfg.getBooleanValue(ConfigKeys.get("VERBOSE"));
  }

  public static String configPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("CONFIG_PATH"));
  }

  public static String topologyName(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"));
  }

  public static String uploaderClass(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("UPLOADER_CLASS"));
  }

  public static String launcherClass(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("LAUNCHER_CLASS"));
  }

  public static String schedulerClass(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_CLASS"));
  }

  public static String runtimeManagerClass(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("RUNTIME_MANAGER_CLASS"));
  }

  public static String packingClass(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("PACKING_CLASS"));
  }

  public static String stateManagerClass(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"));
  }

  public static String clusterFile(Config cfg) {
    return cfg.getStringValue(Keys.clusterFile());
  }

  public static String defaultsFile(Config cfg) {
    return cfg.getStringValue(Keys.defaultsFile());
  }

  public static String metricsSinksFile(Config cfg) {
    return cfg.getStringValue(Keys.metricsSinksFile());
  }

  public static String packingFile(Config cfg) {
    return cfg.getStringValue(Keys.packingFile());
  }

  public static String schedulerFile(Config cfg) {
    return cfg.getStringValue(Keys.schedulerFile());
  }

  public static String stateManagerFile(Config cfg) {
    return cfg.getStringValue(Keys.stateManagerFile());
  }

  public static String systemFile(Config cfg) {
    return cfg.getStringValue(Keys.systemFile());
  }

  public static String uploaderFile(Config cfg) {
    return cfg.getStringValue(Keys.uploaderFile());
  }

  public static String executorBinary(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("EXECUTOR_BINARY"));
  }

  public static String stmgrBinary(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("STMGR_BINARY"));
  }

  public static String tmasterBinary(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("TMASTER_BINARY"));
  }

  public static String shellBinary(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("SHELL_BINARY"));
  }

  public static String schedulerJar(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_JAR"));
  }

  public static String stateManagerConnectionString(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATEMGR_CONNECTION_STRING"));
  }

  public static String stateManagerRootPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATEMGR_ROOT_PATH"));
  }

  public static String corePackageUri(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("CORE_PACKAGE_URI"));
  }

  public static String logDirectory(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("LOGGING_DIRECTORY"));
  }
 
  public static String systemConfigFile(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("SYSTEM_YAML"));
  }

  public static String topologyDefinitionFile(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_DEFINITION_FILE"));
  }

  public static String topologyJarFile(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_JAR_FILE"));
  }

  public static String topologyPackageFile(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_PACKAGE_FILE"));
  }

  public static String topologyPackageType(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_PACKAGE_TYPE"));
  }

  public static Long stmgrRam(Config cfg) {
    return cfg.getLongValue(ConfigKeys.get("STMGR_RAM"));
  }

  public static Long instanceRam(Config cfg) {
    return cfg.getLongValue(ConfigKeys.get("INSTANCE_RAM"));
  }

  public static Double instanceCpu(Config cfg) {
    return cfg.getDoubleValue(ConfigKeys.get("INSTANCE_CPU"));
  }

  public static Long instanceDisk(Config cfg) {
    return cfg.getLongValue(ConfigKeys.get("INSTANCE_DISK"));
  }

  public static String heronHome(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_HOME")); 
  }

  public static String heronBin(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_BIN")); 
  }
  
  public static String heronConf(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_CONF"));
  }

  public static final String heronLib(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_LIB"));
  }

  public static final String heronDist(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_DIST"));
  }

  public static final String heronEtc(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_ETC"));
  }

  public static final String instanceClassPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("INSTANCE_CLASSPATH"));
  }

  public static final String metricsManagerClassPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("METRICSMGR_CLASSPATH"));
  }

  public static final String packingClassPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("PACKING_CLASSPATH"));
  }

  public static final String schedulerClassPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_CLASSPATH"));
  }

  public static final String stateManagerClassPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATEMGR_CLASSPATH"));
  }

  public static final String uploaderClassPath(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("UPLOADER_CLASSPATH"));
  }

  public static final String javaHome(Config cfg) {
    return cfg.getStringValue(ConfigKeys.get("JAVA_HOME"));
  }

  public static final Boolean isZkStateManager(Config cfg) {
    String stateManagerClass = cfg.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"));
    return stateManagerClass.equals(Constants.ZK_STATE_MANAGER_CLASS) ? true : false;
  }
}
