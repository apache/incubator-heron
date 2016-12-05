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

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.common.basics.ByteAmount;

public class ConfigDefaultsTest {

  @Test
  public void testHeronEnviron() throws Exception {
    Assert.assertEquals(
        "/usr/local/heron",
        ConfigDefaults.get("HERON_HOME")
    );
    Assert.assertEquals(
        "${HERON_HOME}/bin",
        ConfigDefaults.get("HERON_BIN")
    );
    Assert.assertEquals(
        "${HERON_HOME}/conf",
        ConfigDefaults.get("HERON_CONF")
    );
    Assert.assertEquals(
        "${HERON_HOME}/dist",
        ConfigDefaults.get("HERON_DIST")
    );
    Assert.assertEquals(
        "${HERON_HOME}/etc",
        ConfigDefaults.get("HERON_ETC")
    );
    Assert.assertEquals(
        "${HERON_HOME}/lib",
        ConfigDefaults.get("HERON_LIB")
    );
    Assert.assertEquals(
        "${JAVA_HOME}",
        ConfigDefaults.get("JAVA_HOME")
    );
  }

  @Test
  public void testConfigFiles() throws Exception {
    Assert.assertEquals(
        "${HERON_CONF}/cluster.yaml",
        ConfigDefaults.get("CLUSTER_YAML")
    );
    Assert.assertEquals(
        "${HERON_CONF}/defaults.yaml",
        ConfigDefaults.get("DEFAULTS_YAML")
    );
    Assert.assertEquals(
        "${HERON_CONF}/metrics_sinks.yaml",
        ConfigDefaults.get("METRICS_YAML")
    );
    Assert.assertEquals(
        "${HERON_CONF}/packing.yaml",
        ConfigDefaults.get("PACKING_YAML")
    );
    Assert.assertEquals(
        "${HERON_CONF}/scheduler.yaml",
        ConfigDefaults.get("SCHEDULER_YAML")
    );
    Assert.assertEquals(
        "${HERON_CONF}/statemgr.yaml",
        ConfigDefaults.get("STATEMGR_YAML")
    );
    Assert.assertEquals(
        "${HERON_CONF}/heron_internals.yaml",
        ConfigDefaults.get("SYSTEM_YAML")
    );
    Assert.assertEquals(
        "${HERON_CONF}/uploader.yaml",
        ConfigDefaults.get("UPLOADER_YAML")
    );
  }

  @Test
  public void testBinaries() throws Exception {
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-executor",
        ConfigDefaults.get("SANDBOX_EXECUTOR_BINARY")
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-stmgr",
        ConfigDefaults.get("SANDBOX_STMGR_BINARY")
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-tmaster",
        ConfigDefaults.get("SANDBOX_TMASTER_BINARY")
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-shell",
        ConfigDefaults.get("SANDBOX_SHELL_BINARY")
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-python-instance",
        ConfigDefaults.get("SANDBOX_PYTHON_INSTANCE_BINARY")
    );
    Assert.assertEquals(
        "heron.jars.scheduler",
        "${HERON_LIB}/scheduler/heron-scheduler.jar",
        ConfigDefaults.get("SCHEDULER_JAR")
    );
  }

  @Test
  public void testClassPaths() throws Exception {
    Assert.assertEquals(
        "${HERON_LIB}/instance/*",
        ConfigDefaults.get("INSTANCE_CLASSPATH")
    );
    Assert.assertEquals(
        "${HERON_LIB}/metricsmgr/*",
        ConfigDefaults.get("METRICSMGR_CLASSPATH")
    );
    Assert.assertEquals(
        "${HERON_LIB}/packing/*",
        ConfigDefaults.get("PACKING_CLASSPATH")
    );
    Assert.assertEquals(
        "${HERON_LIB}/scheduler/*",
        ConfigDefaults.get("SCHEDULER_CLASSPATH")
    );
    Assert.assertEquals(
        "${HERON_LIB}/statemgr/*",
        ConfigDefaults.get("STATEMGR_CLASSPATH")
    );
    Assert.assertEquals(
        "${HERON_LIB}/uploader/*",
        ConfigDefaults.get("UPLOADER_CLASSPATH")
    );
  }

  @Test
  public void testResources() throws Exception {
    Assert.assertEquals(ByteAmount.fromGigabytes(1), ConfigDefaults.getByteAmount("STMGR_RAM"));
    Assert.assertEquals(1.0, ConfigDefaults.getDouble("INSTANCE_CPU"), 0.001);
    Assert.assertEquals(ByteAmount.fromGigabytes(1), ConfigDefaults.getByteAmount("INSTANCE_RAM"));
    Assert.assertEquals(ByteAmount.fromGigabytes(1), ConfigDefaults.getByteAmount("INSTANCE_DISK"));
  }
}
