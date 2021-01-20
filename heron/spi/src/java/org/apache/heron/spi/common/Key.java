/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.spi.common;

import org.apache.heron.common.basics.ByteAmount;

/**
 * Enum of all configuration key values. The following methods exist:
 *
 * name() - return a string representation of the member name (e.g. HERON_HOME)
 * value() - return a key value bound to the enum (e.g. heron.directory.home)
 * getDefault() - return the default value bound to the enum
 * getType() - return the type of the key entry
 */
@SuppressWarnings({"checkstyle:MethodParamPad", "checkstyle:LineLength"})
public enum Key {

  //keys for heron environment
  HERON_HOME               ("heron.directory.home",      "/usr/local/heron"),
  HERON_BIN                ("heron.directory.bin",       "${HERON_HOME}/bin"),
  HERON_CONF               ("heron.directory.conf",      "${HERON_HOME}/conf"),
  HERON_LIB                ("heron.directory.lib",       "${HERON_HOME}/lib"),
  HERON_DIST               ("heron.directory.dist",      "${HERON_HOME}/dist"),
  HERON_ETC                ("heron.directory.etc",       "${HERON_HOME}/etc"),
  JAVA_HOME                ("heron.directory.java.home", "${JAVA_HOME}"),

  //keys for heron configuration files
  CLUSTER_YAML             ("heron.config.file.cluster.yaml",    "${HERON_CONF}/cluster.yaml"),
  CLIENT_YAML              ("heron.config.file.client.yaml",     "${HERON_CONF}/client.yaml"),
  HEALTHMGR_YAML           ("heron.config.file.healthmgr.yaml",  "${HERON_CONF}/healthmgr.yaml"),
  METRICS_YAML             ("heron.config.file.metrics.yaml",    "${HERON_CONF}/metrics_sinks.yaml"),
  PACKING_YAML             ("heron.config.file.packing.yaml",    "${HERON_CONF}/packing.yaml"),
  SCHEDULER_YAML           ("heron.config.file.scheduler.yaml",  "${HERON_CONF}/scheduler.yaml"),
  STATEMGR_YAML            ("heron.config.file.statemgr.yaml",   "${HERON_CONF}/statemgr.yaml"),
  SYSTEM_YAML              ("heron.config.file.system.yaml",     "${HERON_CONF}/heron_internals.yaml"),
  UPLOADER_YAML            ("heron.config.file.uploader.yaml",   "${HERON_CONF}/uploader.yaml"),
  DOWNLOADER_YAML          ("heron.config.file.downloader.yaml", "${HERON_CONF}/downloader.yaml"),
  STATEFUL_YAML            ("heron.config.file.stateful.yaml",   "${HERON_CONF}/stateful.yaml"),

  //keys for config provided in the command line
  CLUSTER                  ("heron.config.cluster",             Type.STRING),
  ROLE                     ("heron.config.role",                Type.STRING),
  ENVIRON                  ("heron.config.environ",             Type.STRING),
  SUBMIT_USER              ("heron.config.submit_user",         Type.STRING),
  DRY_RUN                  ("heron.config.dry_run",             Boolean.FALSE),
  DRY_RUN_FORMAT_TYPE      ("heron.config.dry_run_format_type", Type.DRY_RUN_FORMAT_TYPE),
  VERBOSE                  ("heron.config.verbose",             Boolean.FALSE),
  // Used to enable verbose JVM GC logging
  VERBOSE_GC               ("heron.config.verbose_gc",          Boolean.FALSE),

  CONFIG_PROPERTY          ("heron.config.property",            Type.STRING),

  //keys for release/build information
  BUILD_VERSION            ("heron.build.version",   Type.STRING),
  BUILD_TIME               ("heron.build.time",      Type.STRING),
  BUILD_TIMESTAMP          ("heron.build.timestamp", Type.STRING),
  BUILD_HOST               ("heron.build.host",      Type.STRING),
  BUILD_USER               ("heron.build.user",      Type.STRING),

  //keys for config provided user classes
  UPLOADER_CLASS           ("heron.class.uploader",                        Type.STRING),
  LAUNCHER_CLASS           ("heron.class.launcher",                        Type.STRING),
  SCHEDULER_CLASS          ("heron.class.scheduler",                       Type.STRING),
  PACKING_CLASS            ("heron.class.packing.algorithm",               Type.STRING),
  REPACKING_CLASS          ("heron.class.repacking.algorithm",             Type.STRING),
  STATE_MANAGER_CLASS      ("heron.class.state.manager",                   Type.STRING),
  AURORA_CONTROLLER_CLASS  ("heron.class.scheduler.aurora.controller.cli", Boolean.TRUE),

  //keys for scheduler config
  SCHEDULER_IS_SERVICE     ("heron.scheduler.is.service", Boolean.TRUE),
  SCHEDULER_PROPERTIES     ("heron.scheduler.properties", Type.PROPERTIES),

  //keys for config provided user binaries and jars
  SCHEDULER_JAR            ("heron.jars.scheduler", "${HERON_LIB}/scheduler/heron-scheduler.jar"),

  //keys for config provided files and directories
  INTERNALS_CONFIG_FILE    ("heron.internals.config.file", Type.STRING),

  // heron core can be either a directory or URI, a switch to control it
  // default is to use core URI
  CORE_PACKAGE_DIRECTORY   ("heron.package.core.directory",   "${HERON_DIST}/heron-core"),
  CORE_PACKAGE_URI         ("heron.package.core.uri",         "${HERON_DIST}/heron-core.tar.gz"),
  USE_CORE_PACKAGE_URI     ("heron.package.use_core_uri",     Boolean.TRUE),

  //keys for packages URIs
  TOPOLOGY_PACKAGE_URI     ("heron.package.topology.uri",     Type.STRING),

  //keys for topology
  TOPOLOGY_ID              ("heron.topology.id",              Type.STRING),
  TOPOLOGY_NAME            ("heron.topology.name",            Type.STRING),
  TOPOLOGY_DEFINITION_FILE ("heron.topology.definition.file", Type.STRING),
  TOPOLOGY_DEFINITION      ("heron.topology.definition",      Type.STRING),
  TOPOLOGY_BINARY_FILE     ("heron.topology.binary.file",     Type.STRING),
  TOPOLOGY_PACKAGE_FILE    ("heron.topology.package.file",    Type.STRING),
  TOPOLOGY_PACKAGE_TYPE    ("heron.topology.package.type",    Type.PACKAGE_TYPE),
  TOPOLOGY_CONTAINER_ID    ("heron.topology.container.id",    Type.STRING),

  //keys for proxy config during submission
  SCHEDULER_PROXY_CONNECTION_STRING("heron.proxy.connection.string", Type.STRING),
  SCHEDULER_PROXY_CONNECTION_TYPE  ("heron.proxy.connection.type",   Type.STRING),

  //keys for storing state"),
  STATEMGR_CONNECTION_STRING("heron.statemgr.connection.string", Type.STRING),
  STATEMGR_ROOT_PATH        ("heron.statemgr.root.path",         Type.STRING),

  //keys for config provided default values for resources
  STMGR_RAM                 ("heron.resources.stmgr.ram",      ByteAmount.fromBytes(1073741824)),
  CKPTMGR_RAM               ("heron.resources.ckptmgr.ram",    ByteAmount.fromBytes(1073741824)),
  METRICSMGR_RAM            ("heron.resources.metricsmgr.ram", ByteAmount.fromBytes(1073741824)),
  INSTANCE_RAM              ("heron.resources.instance.ram",   ByteAmount.fromBytes(1073741824)),
  INSTANCE_CPU              ("heron.resources.instance.cpu",   1.0),
  INSTANCE_DISK             ("heron.resources.instance.disk",  ByteAmount.fromBytes(1073741824)),

  //keys for checkpoint management
  STATEFUL_STORAGE_CLASSNAME               ("heron.statefulstorage.classname", Type.STRING),
  STATEFUL_STORAGE_CONF                    ("heron.statefulstorage.config", Type.MAP),
  STATEFUL_STORAGE_CUSTOM_CLASSPATH        ("heron.statefulstorage.custom.classpath", Type.STRING),

  // keys for metricscache manager
  METRICSCACHEMGR_MODE       ("heron.topology.metricscachemgr.mode", "disabled"),
  // keys for health manager
  HEALTHMGR_MODE             ("heron.topology.healthmgr.mode", Type.STRING),

  //keys for config provided paths
  INSTANCE_CLASSPATH         ("heron.classpath.instance",             "${HERON_LIB}/instance/*"),
  HEALTHMGR_CLASSPATH        ("heron.classpath.healthmgr",            "${HERON_LIB}/healthmgr/*"),
  METRICSMGR_CLASSPATH       ("heron.classpath.metrics.manager",      "${HERON_LIB}/metricsmgr/*"),
  METRICSCACHEMGR_CLASSPATH  ("heron.classpath.metricscache.manager", "${HERON_LIB}/metricscachemgr/*"),
  PACKING_CLASSPATH          ("heron.classpath.packing",              "${HERON_LIB}/packing/*"),
  SCHEDULER_CLASSPATH        ("heron.classpath.scheduler",            "${HERON_LIB}/scheduler/*"),
  STATEMGR_CLASSPATH         ("heron.classpath.statemgr",             "${HERON_LIB}/statemgr/*"),
  UPLOADER_CLASSPATH         ("heron.classpath.uploader",             "${HERON_LIB}/uploader/*"),
  CKPTMGR_CLASSPATH          ("heron.classpath.ckptmgr",              "${HERON_LIB}/ckptmgr/*"),
  STATEFULSTORAGE_CLASSPATH  ("heron.classpath.statefulstorage",      "${HERON_LIB}/statefulstorage/*"),

  //keys for run time config
  TOPOLOGY_CLASSPATH             ("heron.runtime.topology.class.path",             Type.STRING),
  SCHEDULER_STATE_MANAGER_ADAPTOR("heron.runtime.scheduler.state.manager.adaptor", Type.STRING),
  SCHEDULER_SHUTDOWN             ("heron.runtime.scheduler.shutdown",              Type.STRING),
  PACKING_CLASS_INSTANCE         ("heron.runtime.packing.class.instance",          Type.STRING),
  LAUNCHER_CLASS_INSTANCE        ("heron.runtime.launcher.class.instance",         Type.STRING),
  COMPONENT_RAMMAP               ("heron.runtime.component.rammap",                Type.STRING),
  COMPONENT_JVM_OPTS_IN_BASE64   ("heron.runtime.component.jvm.opts.in.base64",    Type.STRING),
  INSTANCE_JVM_OPTS_IN_BASE64    ("heron.runtime.instance.jvm.opts.in.base64",     Type.STRING),
  NUM_CONTAINERS                 ("heron.runtime.num.containers",                  Type.INTEGER),
  DOWNLOADER_PROTOCOLS           ("heron.downloader.registry",                     Type.MAP),

  //release info
  HERON_RELEASE_PACKAGE          ("heron.release.package",         Type.STRING),
  HERON_RELEASE_PACKAGE_ROLE     ("heron.release.package.role",    Type.STRING),
  HERON_RELEASE_PACKAGE_NAME     ("heron.release.package.name",    Type.STRING),
  HERON_RELEASE_PACKAGE_VERSION  ("heron.release.package.version", Type.STRING),
  HERON_UPLOADER_VERSION         ("heron.uploader.version",        Type.STRING),

  //keys for config provided paths
  HERON_CLUSTER_HOME     ("heron.directory.cluster.home",      "./heron-core"),
  HERON_CLUSTER_CONF     ("heron.directory.cluster.conf",      "./heron-conf"),
  // TODO: rename below to heron.directory.cluster.java.home, coordinate change with twitter configs
  HERON_CLUSTER_JAVA_HOME("heron.directory.sandbox.java.home", "/usr/lib/jvm/default-java"),

  //keys for heron configuration files on the cluster
  OVERRIDE_YAML("heron.config.file.override.yaml",  "${HERON_CONF}/override.yaml"),

  // Path to the config overrides passed into the API server. Only applicable to submitting
  // topologies via API server
  APISERVER_OVERRIDE_YAML("heron.apiserver.file.override.yaml", Type.STRING),

  //keys for config provided user binaries
  EXECUTOR_BINARY       ("heron.binaries.executor",        "${HERON_BIN}/heron-executor"),
  STMGR_BINARY          ("heron.binaries.stmgr",           "${HERON_BIN}/heron-stmgr"),
  TMANAGER_BINARY       ("heron.binaries.tmanager",        "${HERON_BIN}/heron-tmanager"),
  SHELL_BINARY          ("heron.binaries.shell",           "${HERON_BIN}/heron-shell"),
  PYTHON_INSTANCE_BINARY("heron.binaries.python.instance", "${HERON_BIN}/heron-python-instance"),
  CPP_INSTANCE_BINARY   ("heron.binaries.cpp.instance",    "${HERON_BIN}/heron-cpp-instance"),
  DOWNLOADER_BINARY     ("heron.binaries.downloader",      "${HERON_BIN}/heron-downloader"),
  DOWNLOADER_CONF       ("heron.binaries.downloader-conf", "${HERON_BIN}/heron-downloader-config"),

  // keys for `heron` command line.
  // Prompt user when more containers are required so that
  // user has another chance to double check quota is available.
  // To enable it, change the config from "disabled" to "prompt".
  UPDATE_PROMPT         ("heron.command.update.prompt", "disabled");

  private final String value;
  private final Object defaultValue;
  private final Type type;

  public enum Type {
    BOOLEAN,
    BYTE_AMOUNT,
    DOUBLE,
    DRY_RUN_FORMAT_TYPE,
    INTEGER,
    LONG,
    STRING,
    PACKAGE_TYPE,
    PROPERTIES,
    MAP,
    UNKNOWN
  }

  Key(String value, Type type) {
    this.value = value;
    this.type = type;
    this.defaultValue = null;
  }

  Key(String value, String defaultValue) {
    this.value = value;
    this.type = Type.STRING;
    this.defaultValue = defaultValue;
  }

  Key(String value, Double defaultValue) {
    this.value = value;
    this.type = Type.DOUBLE;
    this.defaultValue = defaultValue;
  }

  Key(String value, Boolean defaultValue) {
    this.value = value;
    this.type = Type.BOOLEAN;
    this.defaultValue = defaultValue;
  }

  Key(String value, ByteAmount defaultValue) {
    this.value = value;
    this.type = Type.BYTE_AMOUNT;
    this.defaultValue = defaultValue;
  }

  /**
   * Get the key value for this enum (i.e., heron.directory.home)
   * @return key value
   */
  public String value() {
    return value;
  }

  public Type getType() {
    return type;
  }

  /**
   * Return the default value
   */
  public Object getDefault() {
    return this.defaultValue;
  }

  public String getDefaultString() {
    if (type != Type.STRING) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultString() not supported", this.name(), this.type));
    }
    return (String) this.defaultValue;
  }
}
