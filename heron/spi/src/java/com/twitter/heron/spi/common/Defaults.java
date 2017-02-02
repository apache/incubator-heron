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

public class Defaults {

  protected Defaults() {
  }

  public static String cluster() {
    return ConfigDefaults.get("CLUSTER");
  }

  public static String role() {
    return ConfigDefaults.get("ROLE");
  }

  public static String environ() {
    return ConfigDefaults.get("ENVIRON");
  }

  public static Boolean verbose() {
    return ConfigDefaults.getBoolean("VERBOSE");
  }

  public static String configPath() {
    return ConfigDefaults.get("CONFIG_PATH");
  }

  public static String topologyName() {
    return ConfigDefaults.get("TOPOLOGY_NAME");
  }

  static Boolean schedulerService() {
    return ConfigDefaults.getBoolean("SCHEDULER_IS_SERVICE");
  }

  static String clusterFile() {
    return ConfigDefaults.get("CLUSTER_YAML");
  }

  static String clientFile() {
    return ConfigDefaults.get("CLIENT_YAML");
  }

  static String defaultsFile() {
    return ConfigDefaults.get("DEFAULTS_YAML");
  }

  static String metricsSinksFile() {
    return ConfigDefaults.get("METRICS_YAML");
  }

  static String packingFile() {
    return ConfigDefaults.get("PACKING_YAML");
  }

  static String schedulerFile() {
    return ConfigDefaults.get("SCHEDULER_YAML");
  }

  static String stateManagerFile() {
    return ConfigDefaults.get("STATEMGR_YAML");
  }

  static String systemFile() {
    return ConfigDefaults.get("SYSTEM_YAML");
  }

  static String uploaderFile() {
    return ConfigDefaults.get("UPLOADER_YAML");
  }

  static String schedulerJar() {
    return ConfigDefaults.get("SCHEDULER_JAR");
  }

  static String stateManagerConnectionString() {
    return ConfigDefaults.get("STATEMGR_CONNECTION_STRING");
  }

  static String stateManagerRootPath() {
    return ConfigDefaults.get("STATEMGR_ROOT_PATH");
  }

  static String corePackageUri() {
    return ConfigDefaults.get("CORE_PACKAGE_URI");
  }

  static String topologyDefinitionFile() {
    return ConfigDefaults.get("TOPOLOGY_DEFINITION_FILE");
  }

  static String topologyBinaryFile() {
    return ConfigDefaults.get("TOPOLOGY_BINARY_FILE");
  }

  static String topologyPackageFile() {
    return ConfigDefaults.get("TOPOLOGY_PACKAGE_FILE");
  }

  static PackageType topologyPackageType() {
    return ConfigDefaults.getPackageType("TOPOLOGY_PACKAGE_TYPE");
  }

  static DryRunFormatType dryRunFormatType() {
    return ConfigDefaults.getDryRunFormatType("DRY_RUN_FORMAT_TYPE");
  }

  static ByteAmount stmgrRam() {
    return ConfigDefaults.getByteAmount("STMGR_RAM");
  }

  static ByteAmount instanceRam() {
    return ConfigDefaults.getByteAmount("INSTANCE_RAM");
  }

  static Double instanceCpu() {
    return ConfigDefaults.getDouble("INSTANCE_CPU");
  }

  static ByteAmount instanceDisk() {
    return ConfigDefaults.getByteAmount("INSTANCE_DISK");
  }

  static String heronHome() {
    return ConfigDefaults.get("HERON_HOME");
  }

  static String heronBin() {
    return ConfigDefaults.get("HERON_BIN");
  }

  static String heronConf() {
    return ConfigDefaults.get("HERON_CONF");
  }

  static String heronLib() {
    return ConfigDefaults.get("HERON_LIB");
  }

  static String heronDist() {
    return ConfigDefaults.get("HERON_DIST");
  }

  static String heronEtc() {
    return ConfigDefaults.get("HERON_ETC");
  }

  static String instanceClassPath() {
    return ConfigDefaults.get("INSTANCE_CLASSPATH");
  }

  static String metricsManagerClassPath() {
    return ConfigDefaults.get("METRICSMGR_CLASSPATH");
  }

  static String packingClassPath() {
    return ConfigDefaults.get("PACKING_CLASSPATH");
  }

  static String schedulerClassPath() {
    return ConfigDefaults.get("SCHEDULER_CLASSPATH");
  }

  static String stateManagerClassPath() {
    return ConfigDefaults.get("STATEMGR_CLASSPATH");
  }

  static String uploaderClassPath() {
    return ConfigDefaults.get("UPLOADER_CLASSPATH");
  }

  static String javaHome() {
    return ConfigDefaults.get("JAVA_HOME");
  }

  static String heronSandboxHome() {
    return ConfigDefaults.get("HERON_SANDBOX_HOME");
  }

  static String heronSandboxBin() {
    return ConfigDefaults.get("HERON_SANDBOX_BIN");
  }

  static String heronSandboxConf() {
    return ConfigDefaults.get("HERON_SANDBOX_CONF");
  }

  static String heronSandboxLib() {
    return ConfigDefaults.get("HERON_SANDBOX_LIB");
  }

  static String javaSandboxHome() {
    return ConfigDefaults.get("HERON_SANDBOX_JAVA_HOME");
  }

  static String clusterSandboxFile() {
    return ConfigDefaults.get("SANDBOX_CLUSTER_YAML");
  }

  static String defaultsSandboxFile() {
    return ConfigDefaults.get("SANDBOX_DEFAULTS_YAML");
  }

  static String metricsSinksSandboxFile() {
    return ConfigDefaults.get("SANDBOX_METRICS_YAML");
  }

  static String packingSandboxFile() {
    return ConfigDefaults.get("SANDBOX_PACKING_YAML");
  }

  static String schedulerSandboxFile() {
    return ConfigDefaults.get("SANDBOX_SCHEDULER_YAML");
  }

  static String stateManagerSandboxFile() {
    return ConfigDefaults.get("SANDBOX_STATEMGR_YAML");
  }

  static String systemSandboxFile() {
    return ConfigDefaults.get("SANDBOX_SYSTEM_YAML");
  }

  static String uploaderSandboxFile() {
    return ConfigDefaults.get("SANDBOX_UPLOADER_YAML");
  }

  static String overrideSandboxFile() {
    return ConfigDefaults.get("SANDBOX_OVERRIDE_YAML");
  }

  static String executorSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_EXECUTOR_BINARY");
  }

  static String stmgrSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_STMGR_BINARY");
  }

  static String tmasterSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_TMASTER_BINARY");
  }

  static String shellSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_SHELL_BINARY");
  }

  static String pythonInstanceSandboxBinary() {
    return ConfigDefaults.get("SANDBOX_PYTHON_INSTANCE_BINARY");
  }

  static String schedulerSandboxJar() {
    return ConfigDefaults.get("SANDBOX_SCHEDULER_JAR");
  }

  static String instanceSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_INSTANCE_CLASSPATH");
  }

  static String metricsManagerSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_METRICSMGR_CLASSPATH");
  }

  static String packingSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_PACKING_CLASSPATH");
  }

  static String schedulerSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_SCHEDULER_CLASSPATH");
  }

  static String stateManagerSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_STATEMGR_CLASSPATH");
  }

  static String uploaderSandboxClassPath() {
    return ConfigDefaults.get("SANDBOX_UPLOADER_CLASSPATH");
  }
}
