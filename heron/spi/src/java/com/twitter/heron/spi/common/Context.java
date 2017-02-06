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

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.DryRunFormatType;
import com.twitter.heron.common.basics.PackageType;

public class Context {

  protected Context() {
  }

  public static String cluster(Config cfg) {
    return cfg.getStringValue(Key.CLUSTER);
  }

  public static String role(Config cfg) {
    return cfg.getStringValue(Key.ROLE);
  }

  public static String environ(Config cfg) {
    return cfg.getStringValue(Key.ENVIRON);
  }

  public static Boolean dryRun(Config cfg) {
    return cfg.getBooleanValue(Key.DRY_RUN, false);
  }

  public static DryRunFormatType dryRunFormatType(Config cfg) {
    return cfg.getDryRunFormatType(Key.DRY_RUN_FORMAT_TYPE);
  }

  public static Boolean verbose(Config cfg) {
    return cfg.getBooleanValue(Key.VERBOSE, true);
  }

  public static String configPath(Config cfg) {
    return cfg.getStringValue(Key.CONFIG_PATH);
  }

  public static String buildVersion(Config cfg) {
    return cfg.getStringValue(Key.BUILD_VERSION);
  }

  public static String buildTime(Config cfg) {
    return cfg.getStringValue(Key.BUILD_TIME);
  }

  public static Long buildTimeStamp(Config cfg) {
    return cfg.getLongValue(Key.BUILD_TIMESTAMP);
  }

  public static String buildHost(Config cfg) {
    return cfg.getStringValue(Key.BUILD_HOST);
  }

  public static String buildUser(Config cfg) {
    return cfg.getStringValue(Key.BUILD_USER);
  }

  public static String topologyName(Config cfg) {
    return cfg.getStringValue(Key.TOPOLOGY_NAME);
  }

  public static int topologyContainerId(Config cfg) {
    return cfg.getIntegerValue(Key.TOPOLOGY_CONTAINER_ID);
  }

  public static String uploaderClass(Config cfg) {
    return cfg.getStringValue(Key.UPLOADER_CLASS);
  }

  public static String launcherClass(Config cfg) {
    return cfg.getStringValue(Key.LAUNCHER_CLASS);
  }

  public static String schedulerClass(Config cfg) {
    return cfg.getStringValue(Key.SCHEDULER_CLASS);
  }

  public static String packingClass(Config cfg) {
    return cfg.getStringValue(Key.PACKING_CLASS);
  }

  public static String repackingClass(Config cfg) {
    return cfg.getStringValue(Key.REPACKING_CLASS);
  }

  public static String stateManagerClass(Config cfg) {
    return cfg.getStringValue(Key.STATE_MANAGER_CLASS);
  }

  public static Boolean schedulerService(Config cfg) {
    return cfg.getBooleanValue(Key.SCHEDULER_IS_SERVICE, true);
  }

  public static String clusterFile(Config cfg) {
    return cfg.getStringValue(Key.CLUSTER_YAML);
  }

  public static String clientFile(Config cfg) {
    return cfg.getStringValue(Key.CLIENT_YAML);
  }

  public static String defaultsFile(Config cfg) {
    return cfg.getStringValue(Key.DEFAULTS_YAML);
  }

  public static String metricsSinksFile(Config cfg) {
    return cfg.getStringValue(Key.METRICS_YAML);
  }

  public static String packingFile(Config cfg) {
    return cfg.getStringValue(Key.PACKING_YAML);
  }

  public static String schedulerFile(Config cfg) {
    return cfg.getStringValue(Key.SCHEDULER_YAML);
  }

  public static String stateManagerFile(Config cfg) {
    return cfg.getStringValue(Key.STATEMGR_YAML);
  }

  public static String systemFile(Config cfg) {
    return cfg.getStringValue(Key.SYSTEM_YAML);
  }

  public static String uploaderFile(Config cfg) {
    return cfg.getStringValue(Key.UPLOADER_YAML);
  }

  public static String schedulerJar(Config cfg) {
    return cfg.getStringValue(Key.SCHEDULER_JAR);
  }

  public static String schedulerProxyConnectionString(Config cfg) {
    return cfg.getStringValue(Key.SCHEDULER_PROXY_CONNECTION_STRING);
  }

  public static String schedulerProxyConnectionType(Config cfg) {
    return cfg.getStringValue(Key.SCHEDULER_PROXY_CONNECTION_TYPE);
  }

  public static String stateManagerConnectionString(Config cfg) {
    return cfg.getStringValue(Key.STATEMGR_CONNECTION_STRING);
  }

  public static String stateManagerRootPath(Config cfg) {
    return cfg.getStringValue(Key.STATEMGR_ROOT_PATH);
  }

  public static String corePackageUri(Config cfg) {
    return cfg.getStringValue(Key.CORE_PACKAGE_URI);
  }

  public static String systemConfigFile(Config cfg) {
    return cfg.getStringValue(Key.SYSTEM_YAML);
  }

  public static String topologyDefinitionFile(Config cfg) {
    return cfg.getStringValue(Key.TOPOLOGY_DEFINITION_FILE);
  }

  public static String topologyBinaryFile(Config cfg) {
    return cfg.getStringValue(Key.TOPOLOGY_BINARY_FILE);
  }

  public static String topologyPackageFile(Config cfg) {
    return cfg.getStringValue(Key.TOPOLOGY_PACKAGE_FILE);
  }

  public static PackageType topologyPackageType(Config cfg) {
    return cfg.getPackageType(Key.TOPOLOGY_PACKAGE_TYPE);
  }

  public static ByteAmount stmgrRam(Config cfg) {
    return cfg.getByteAmountValue(Key.STMGR_RAM);
  }

  public static ByteAmount instanceRam(Config cfg) {
    return cfg.getByteAmountValue(Key.INSTANCE_RAM);
  }

  public static Double instanceCpu(Config cfg) {
    return cfg.getDoubleValue(Key.INSTANCE_CPU);
  }

  public static ByteAmount instanceDisk(Config cfg) {
    return cfg.getByteAmountValue(Key.INSTANCE_DISK);
  }

  public static String heronHome(Config cfg) {
    return cfg.getStringValue(Key.HERON_HOME);
  }

  public static String heronBin(Config cfg) {
    return cfg.getStringValue(Key.HERON_BIN);
  }

  public static String heronConf(Config cfg) {
    return cfg.getStringValue(Key.HERON_CONF);
  }

  public static final String heronLib(Config cfg) {
    return cfg.getStringValue(Key.HERON_LIB);
  }

  public static final String heronDist(Config cfg) {
    return cfg.getStringValue(Key.HERON_DIST);
  }

  public static final String heronEtc(Config cfg) {
    return cfg.getStringValue(Key.HERON_ETC);
  }

  public static final String instanceClassPath(Config cfg) {
    return cfg.getStringValue(Key.INSTANCE_CLASSPATH);
  }

  public static final String metricsManagerClassPath(Config cfg) {
    return cfg.getStringValue(Key.METRICSMGR_CLASSPATH);
  }

  public static final String packingClassPath(Config cfg) {
    return cfg.getStringValue(Key.PACKING_CLASSPATH);
  }

  public static final String schedulerClassPath(Config cfg) {
    return cfg.getStringValue(Key.SCHEDULER_CLASSPATH);
  }

  public static final String stateManagerClassPath(Config cfg) {
    return cfg.getStringValue(Key.STATEMGR_CLASSPATH);
  }

  public static final String uploaderClassPath(Config cfg) {
    return cfg.getStringValue(Key.UPLOADER_CLASSPATH);
  }

  public static final String javaHome(Config cfg) {
    return cfg.getStringValue(Key.JAVA_HOME);
  }

  public static String heronSandboxHome(Config cfg) {
    return cfg.getStringValue(Key.HERON_SANDBOX_HOME);
  }

  public static String heronSandboxBin(Config cfg) {
    return cfg.getStringValue(Key.HERON_SANDBOX_BIN);
  }

  public static String heronSandboxConf(Config cfg) {
    return cfg.getStringValue(Key.HERON_SANDBOX_CONF);
  }

  public static final String heronSandboxLib(Config cfg) {
    return cfg.getStringValue(Key.HERON_SANDBOX_LIB);
  }

  public static final String javaSandboxHome(Config cfg) {
    return cfg.getStringValue(Key.HERON_SANDBOX_JAVA_HOME);
  }

  public static String clusterSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_CLUSTER_YAML);
  }

  public static String defaultsSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_DEFAULTS_YAML);
  }

  public static String metricsSinksSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_METRICS_YAML);
  }

  public static String packingSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_PACKING_YAML);
  }

  public static String overrideSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_OVERRIDE_YAML);
  }

  public static String schedulerSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_SCHEDULER_YAML);
  }

  public static String stateManagerSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_STATEMGR_YAML);
  }

  public static String systemConfigSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_SYSTEM_YAML);
  }

  public static String uploaderSandboxFile(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_UPLOADER_YAML);
  }

  public static String executorSandboxBinary(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_EXECUTOR_BINARY);
  }

  public static String stmgrSandboxBinary(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_STMGR_BINARY);
  }

  public static String tmasterSandboxBinary(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_TMASTER_BINARY);
  }

  public static String shellSandboxBinary(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_SHELL_BINARY);
  }

  public static final String pythonInstanceSandboxBinary(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_PYTHON_INSTANCE_BINARY);
  }

  public static final String metricsCacheManagerSandboxClassPath(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_METRICSCACHEMGR_CLASSPATH);
  }

  public static String schedulerSandboxJar(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_SCHEDULER_JAR);
  }

  public static final String instanceSandboxClassPath(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_INSTANCE_CLASSPATH);
  }

  public static final String metricsManagerSandboxClassPath(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_METRICSMGR_CLASSPATH);
  }

  public static final String packingSandboxClassPath(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_PACKING_CLASSPATH);
  }

  public static final String schedulerSandboxClassPath(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_SCHEDULER_CLASSPATH);
  }

  public static final String stateManagerSandboxClassPath(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_STATEMGR_CLASSPATH);
  }

  public static final String uploaderSandboxClassPath(Config cfg) {
    return cfg.getStringValue(Key.SANDBOX_UPLOADER_CLASSPATH);
  }
}
