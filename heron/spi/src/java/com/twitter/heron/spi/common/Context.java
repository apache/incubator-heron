// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spi.common;

public class Context {

  protected Context() {
  }

  public static String cluster(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("CLUSTER"));
  }

  public static String role(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("ROLE"));
  }

  public static String environ(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("ENVIRON"));
  }

  public static Boolean verbose(SpiCommonConfig cfg) {
    return cfg.getBooleanValue(ConfigKeys.get("VERBOSE"), true);
  }

  public static String configPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("CONFIG_PATH"));
  }

  public static String buildVersion(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("BUILD_VERSION"));
  }

  public static String buildTime(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("BUILD_TIME"));
  }

  public static Long buildTimeStamp(SpiCommonConfig cfg) {
    return cfg.getLongValue(ConfigKeys.get("BUILD_TIMESTAMP"));
  }

  public static String buildHost(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("BUILD_HOST"));
  }

  public static String buildUser(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("BUILD_USER"));
  }

  public static String topologyName(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"));
  }

  public static int topologyContainerId(SpiCommonConfig cfg) {
    return cfg.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"));
  }

  public static String uploaderClass(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("UPLOADER_CLASS"));
  }

  public static String launcherClass(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("LAUNCHER_CLASS"));
  }

  public static String schedulerClass(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_CLASS"));
  }

  public static String packingClass(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("PACKING_CLASS"));
  }

  public static String stateManagerClass(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"));
  }

  public static Boolean schedulerService(SpiCommonConfig cfg) {
    return cfg.getBooleanValue(ConfigKeys.get("SCHEDULER_IS_SERVICE"), true);
  }

  public static String clusterFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.clusterFile());
  }

  public static String clientFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.clientFile());
  }

  public static String defaultsFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.defaultsFile());
  }

  public static String metricsSinksFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.metricsSinksFile());
  }

  public static String packingFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.packingFile());
  }

  public static String schedulerFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.schedulerFile());
  }

  public static String stateManagerFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.stateManagerFile());
  }

  public static String systemFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.systemFile());
  }

  public static String uploaderFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.uploaderFile());
  }

  public static String schedulerJar(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_JAR"));
  }

  public static String schedulerProxyConnectionString(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_PROXY_CONNECTION_STRING"));
  }

  public static String schedulerProxyConnectionType(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_PROXY_CONNECTION_TYPE"));
  }

  public static String stateManagerConnectionString(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATEMGR_CONNECTION_STRING"));
  }

  public static String stateManagerRootPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATEMGR_ROOT_PATH"));
  }

  public static String corePackageUri(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("CORE_PACKAGE_URI"));
  }

  public static String systemConfigFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SYSTEM_YAML"));
  }

  public static String topologyDefinitionFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_DEFINITION_FILE"));
  }

  public static String topologyJarFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_JAR_FILE"));
  }

  public static String topologyPackageFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_PACKAGE_FILE"));
  }

  public static String topologyPackageType(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("TOPOLOGY_PACKAGE_TYPE"));
  }

  public static Long stmgrRam(SpiCommonConfig cfg) {
    return cfg.getLongValue(ConfigKeys.get("STMGR_RAM"));
  }

  public static Long instanceRam(SpiCommonConfig cfg) {
    return cfg.getLongValue(ConfigKeys.get("INSTANCE_RAM"));
  }

  public static Double instanceCpu(SpiCommonConfig cfg) {
    return cfg.getDoubleValue(ConfigKeys.get("INSTANCE_CPU"));
  }

  public static Long instanceDisk(SpiCommonConfig cfg) {
    return cfg.getLongValue(ConfigKeys.get("INSTANCE_DISK"));
  }

  public static String heronHome(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_HOME"));
  }

  public static String heronBin(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_BIN"));
  }

  public static String heronConf(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_CONF"));
  }

  public static final String heronLib(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_LIB"));
  }

  public static final String heronDist(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_DIST"));
  }

  public static final String heronEtc(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_ETC"));
  }

  public static final String instanceClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("INSTANCE_CLASSPATH"));
  }

  public static final String metricsManagerClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("METRICSMGR_CLASSPATH"));
  }

  public static final String packingClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("PACKING_CLASSPATH"));
  }

  public static final String schedulerClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SCHEDULER_CLASSPATH"));
  }

  public static final String stateManagerClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("STATEMGR_CLASSPATH"));
  }

  public static final String uploaderClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("UPLOADER_CLASSPATH"));
  }

  public static final String javaHome(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("JAVA_HOME"));
  }

  public static String heronSandboxHome(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_SANDBOX_HOME"));
  }

  public static String heronSandboxBin(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_SANDBOX_BIN"));
  }

  public static String heronSandboxConf(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_SANDBOX_CONF"));
  }

  public static final String heronSandboxLib(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_SANDBOX_LIB"));
  }

  public static final String javaSandboxHome(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("HERON_SANDBOX_JAVA_HOME"));
  }

  public static String clusterSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.clusterSandboxFile());
  }

  public static String defaultsSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.defaultsSandboxFile());
  }

  public static String metricsSinksSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.metricsSinksSandboxFile());
  }

  public static String packingSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.packingSandboxFile());
  }

  public static String overrideSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.overrideSandboxFile());
  }

  public static String schedulerSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.schedulerSandboxFile());
  }

  public static String stateManagerSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.stateManagerSandboxFile());
  }

  public static String systemConfigSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.systemSandboxFile());
  }

  public static String uploaderSandboxFile(SpiCommonConfig cfg) {
    return cfg.getStringValue(Keys.uploaderSandboxFile());
  }

  public static String executorSandboxBinary(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_EXECUTOR_BINARY"));
  }

  public static String stmgrSandboxBinary(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_STMGR_BINARY"));
  }

  public static String tmasterSandboxBinary(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_TMASTER_BINARY"));
  }

  public static String shellSandboxBinary(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_SHELL_BINARY"));
  }

  public static String schedulerSandboxJar(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_SCHEDULER_JAR"));
  }

  public static final String instanceSandboxClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_INSTANCE_CLASSPATH"));
  }

  public static final String metricsManagerSandboxClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_METRICSMGR_CLASSPATH"));
  }

  public static final String packingSandboxClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_PACKING_CLASSPATH"));
  }

  public static final String schedulerSandboxClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_SCHEDULER_CLASSPATH"));
  }

  public static final String stateManagerSandboxClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_STATEMGR_CLASSPATH"));
  }

  public static final String uploaderSandboxClassPath(SpiCommonConfig cfg) {
    return cfg.getStringValue(ConfigKeys.get("SANDBOX_UPLOADER_CLASSPATH"));
  }
}
