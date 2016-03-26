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

  public static String javaHome() {
    return ConfigDefaults.get("JAVA_HOME");
  }

  public static String heronSandboxHome() {
    return ConfigDefaults.get("HERON_SANDBOX_HOME");
  }

  public static String heronSandboxBin() {
    return ConfigDefaults.get("HERON_SANDBOX_BIN");
  }

  public static String heronSandboxConf() {
    return ConfigDefaults.get("HERON_SANDBOX_CONF");
  }

  public static String heronSandboxLib() {
    return ConfigDefaults.get("HERON_SANDBOX_LIB");
  }

  public static String javaSandboxHome() {
    return ConfigDefaults.get("HERON_SANDBOX_JAVA_HOME");
  }

  public static String clusterSandboxFile() {
    return ConfigDefaults.get("SANDBOX_CLUSTER_YAML");
  }

  public static String defaultsSandboxFile() {
    return ConfigDefaults.get("SANDBOX_DEFAULTS_YAML");
  }

  public static String metricsSinksSandboxFile() {
    return ConfigDefaults.get("SANDBOX_METRICS_YAML");
  }

  public static String packingSandboxFile() {
    return ConfigDefaults.get("SANDBOX_PACKING_YAML");
  }

  public static String schedulerSandboxFile() {
    return ConfigDefaults.get("SANDBOX_SCHEDULER_YAML");
  }

  public static String stateManagerSandboxFile() {
    return ConfigDefaults.get("SANDBOX_STATEMGR_YAML");
  }

  public static String systemSandboxFile() {
    return ConfigDefaults.get("SANDBOX_SYSTEM_YAML");
  }

  public static String uploaderSandboxFile() {
    return ConfigDefaults.get("SANDBOX_UPLOADER_YAML");
  }

  public static String executorSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_EXECUTOR_BINARY");
  }

  public static String stmgrSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_STMGR_BINARY");
  }

  public static String tmasterSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_TMASTER_BINARY");
  }

  public static String shellSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_SHELL_BINARY");
  }

  public static String schedulerSandboxJar() {
    return ConfigDefaults.get("SANDBOX_SCHEDULER_JAR");
  }

  public static String logSandboxDirectory() {
    return ConfigDefaults.get("SANDBOX_LOGGING_DIRECTORY");
  }

  public static String instanceSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_INSTANCE_CLASSPATH");
  }

  public static String metricsManagerSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_METRICSMGR_CLASSPATH");
  }

  public static String packingSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_PACKING_CLASSPATH");
  }

  public static String schedulerSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_SCHEDULER_CLASSPATH");
  }

  public static String stateManagerSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_STATEMGR_CLASSPATH");
  }

  public static String uploaderSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_UPLOADER_CLASSPATH");
  }
}
