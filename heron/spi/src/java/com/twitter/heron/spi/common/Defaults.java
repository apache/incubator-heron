package com.twitter.heron.spi.common;

public class Defaults {

  public static String cluster() {
    return ConfigDefaults.get("CLUSTER");
  }

  public static String role() {
    return ConfigDefaults.get("ROLE");
  }

  public static String environ() {
    return ConfigDefaults.get("ENVIRON");
  }

  public static String verbose() {
    return ConfigDefaults.get("VERBOSE");
  }

  public static String configPath() {
    return ConfigDefaults.get("CONFIG_PATH");
  }

  public static String topologyName() {
    return ConfigDefaults.get("TOPOLOGY_NAME");
  }

  public static String uploaderClass() {
    return ConfigDefaults.get("UPLOADER_CLASS");
  }

  public static String launcherClass() {
    return ConfigDefaults.get("LAUNCHER_CLASS");
  }

  public static String schedulerClass() {
    return ConfigDefaults.get("SCHEDULER_CLASS");
  }

  public static String runtimeManagerClass() {
    return ConfigDefaults.get("RUNTIME_MANAGER_CLASS");
  }

  public static String packingClass() {
    return ConfigDefaults.get("PACKING_CLASS");
  }

  public static String stateManagerClass() {
    return ConfigDefaults.get("STATE_MANAGER_CLASS");
  }

  public static String clusterFile() {
    return ConfigDefaults.get("CLUSTER_YAML");
  }

  public static String defaultsFile() {
    return ConfigDefaults.get("DEFAULTS_YAML");
  }

  public static String metricsSinksFile() {
    return ConfigDefaults.get("METRICS_YAML");
  }

  public static String packingFile() {
    return ConfigDefaults.get("PACKING_YAML");
  }

  public static String schedulerFile() {
    return ConfigDefaults.get("SCHEDULER_YAML");
  }

  public static String stateManagerFile() {
    return ConfigDefaults.get("STATEMGR_YAML");
  }

  public static String systemFile() {
    return ConfigDefaults.get("SYSTEM_YAML");
  }

  public static String uploaderFile() {
    return ConfigDefaults.get("UPLOADER_YAML");
  }

  public static String executorBinary() {
    return ConfigDefaults.get("EXECUTOR_BINARY");
  }

  public static String stmgrBinary() {
    return ConfigDefaults.get("STMGR_BINARY");
  }

  public static String tmasterBinary() {
    return ConfigDefaults.get("TMASTER_BINARY");
  }

  public static String shellBinary() {
    return ConfigDefaults.get("SHELL_BINARY");
  }

  public static String schedulerJar() {
    return ConfigDefaults.get("SCHEDULER_JAR");
  }

  public static String stateManagerConnectionString() {
    return ConfigDefaults.get("STATEMGR_CONNECTION_STRING");
  }

  public static String stateManagerRootPath() {
    return ConfigDefaults.get("STATEMGR_ROOT_PATH");
  }

  public static String corePackageUri() {
    return ConfigDefaults.get("CORE_PACKAGE_URI");
  }

  public static String logDirectory() {
    return ConfigDefaults.get("LOGGING_DIRECTORY");
  }
 
  public static String systemConfigFile() {
    return ConfigDefaults.get("SYSTEM_YAML");
  }

  public static String topologyDefinitionFile() {
    return ConfigDefaults.get("TOPOLOGY_DEFINITION_FILE");
  }

  public static String topologyJarFile() {
    return ConfigDefaults.get("TOPOLOGY_JAR_FILE");
  }

  public static String topologyPackageFile() {
    return ConfigDefaults.get("TOPOLOGY_PACKAGE_FILE");
  }

  public static String topologyPackageType() {
    return ConfigDefaults.get("TOPOLOGY_PACKAGE_TYPE");
  }

  public static Long stmgrRam() {
    return ConfigDefaults.getLong("STMGR_RAM");
  }

  public static Long instanceRam() {
    return ConfigDefaults.getLong("INSTANCE_RAM");
  }

  public static Double instanceCpu() {
    return ConfigDefaults.getDouble("INSTANCE_CPU");
  }

  public static Long instanceDisk() {
    return ConfigDefaults.getLong("INSTANCE_DISK");
  }

  public static String heronHome() {
    return ConfigDefaults.get("HERON_HOME"); 
  }

  public static String heronBin() {
    return ConfigDefaults.get("HERON_BIN"); 
  }
  
  public static String heronConf() {
    return ConfigDefaults.get("HERON_CONF");
  }

  public static String heronLib() {
    return ConfigDefaults.get("HERON_LIB");
  }

  public static String heronDist() {
    return ConfigDefaults.get("HERON_DIST");
  }

  public static String heronEtc() {
    return ConfigDefaults.get("HERON_ETC");
  }

  public static String instanceClassPath() {
    return ConfigDefaults.get("INSTANCE_CLASSPATH");
  }

  public static String metricsManagerClassPath() {
    return ConfigDefaults.get("METRICSMGR_CLASSPATH");
  }

  public static String packingClassPath() {
    return ConfigDefaults.get("PACKING_CLASSPATH");
  }

  public static String schedulerClassPath() {
    return ConfigDefaults.get("SCHEDULER_CLASSPATH");
  }

  public static String stateManagerClassPath() {
    return ConfigDefaults.get("STATEMGR_CLASSPATH");
  }

  public static String uploaderClassPath() {
    return ConfigDefaults.get("UPLOADER_CLASSPATH");
  }

  public static String sandboxHome() {
    return ConfigDefaults.get("HERON_SANDBOX_HOME");
  }

  public static String sandboxConf() {
    return ConfigDefaults.get("HERON_SANDBOX_CONF");
  }

  public static String javaHome() {
    return ConfigDefaults.get("JAVA_HOME");
  }
}
