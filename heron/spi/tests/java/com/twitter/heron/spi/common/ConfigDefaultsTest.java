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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.common.basics.ByteAmount;

public class ConfigDefaultsTest {

  /*
   * Test to check if all the default keys have a corresponding enum Key
   */
  @Test
  public void testConfigKeysDefaults() throws Exception {
    Map<String, Object> defaults = ConfigDefaults.defaults;

    for (String key : defaults.keySet()) {
      try {
        Key.valueOf(key);
      } catch (IllegalArgumentException e) {
        Assert.fail("ConfigDefaults.defaults contains a key not found in Key enum: " + key);
      }
    }
  }

  @Test
  public void testHeronEnviron() throws Exception {
    Assert.assertEquals(
        "/usr/local/heron",
        ConfigDefaults.get(Key.HERON_HOME)
    );
    Assert.assertEquals(
        "${HERON_HOME}/bin",
        ConfigDefaults.get(Key.HERON_BIN)
    );
    Assert.assertEquals(
        "${HERON_HOME}/conf",
        ConfigDefaults.get(Key.HERON_CONF)
    );
    Assert.assertEquals(
        "${HERON_HOME}/dist",
        ConfigDefaults.get(Key.HERON_DIST)
    );
    Assert.assertEquals(
        "${HERON_HOME}/etc",
        ConfigDefaults.get(Key.HERON_ETC)
    );
    Assert.assertEquals(
        "${HERON_HOME}/lib",
        ConfigDefaults.get(Key.HERON_LIB)
    );
    Assert.assertEquals(
        "${JAVA_HOME}",
        ConfigDefaults.get(Key.JAVA_HOME)
    );
  }

  @Test
  public void testConfigFiles() throws Exception {
    Assert.assertEquals(
        "${HERON_CONF}/cluster.yaml",
        ConfigDefaults.get(Key.CLUSTER_YAML)
    );
    Assert.assertEquals(
        "${HERON_CONF}/defaults.yaml",
        ConfigDefaults.get(Key.DEFAULTS_YAML)
    );
    Assert.assertEquals(
        "${HERON_CONF}/metrics_sinks.yaml",
        ConfigDefaults.get(Key.METRICS_YAML)
    );
    Assert.assertEquals(
        "${HERON_CONF}/packing.yaml",
        ConfigDefaults.get(Key.PACKING_YAML)
    );
    Assert.assertEquals(
        "${HERON_CONF}/scheduler.yaml",
        ConfigDefaults.get(Key.SCHEDULER_YAML)
    );
    Assert.assertEquals(
        "${HERON_CONF}/statemgr.yaml",
        ConfigDefaults.get(Key.STATEMGR_YAML)
    );
    Assert.assertEquals(
        "${HERON_CONF}/heron_internals.yaml",
        ConfigDefaults.get(Key.SYSTEM_YAML)
    );
    Assert.assertEquals(
        "${HERON_CONF}/uploader.yaml",
        ConfigDefaults.get(Key.UPLOADER_YAML)
    );
  }

  @Test
  public void testBinaries() throws Exception {
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-executor",
        ConfigDefaults.get(Key.SANDBOX_EXECUTOR_BINARY)
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-stmgr",
        ConfigDefaults.get(Key.SANDBOX_STMGR_BINARY)
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-tmaster",
        ConfigDefaults.get(Key.SANDBOX_TMASTER_BINARY)
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-shell",
        ConfigDefaults.get(Key.SANDBOX_SHELL_BINARY)
    );
    Assert.assertEquals(
        "${HERON_SANDBOX_BIN}/heron-python-instance",
        ConfigDefaults.get(Key.SANDBOX_PYTHON_INSTANCE_BINARY)
    );
    Assert.assertEquals(
        "heron.jars.scheduler",
        "${HERON_LIB}/scheduler/heron-scheduler.jar",
        ConfigDefaults.get(Key.SCHEDULER_JAR)
    );
  }

  @Test
  public void testClassPaths() throws Exception {
    Assert.assertEquals(
        "${HERON_LIB}/instance/*",
        ConfigDefaults.get(Key.INSTANCE_CLASSPATH)
    );
    Assert.assertEquals(
        "${HERON_LIB}/metricsmgr/*",
        ConfigDefaults.get(Key.METRICSMGR_CLASSPATH)
    );
    Assert.assertEquals(
        "${HERON_LIB}/packing/*",
        ConfigDefaults.get(Key.PACKING_CLASSPATH)
    );
    Assert.assertEquals(
        "${HERON_LIB}/scheduler/*",
        ConfigDefaults.get(Key.SCHEDULER_CLASSPATH)
    );
    Assert.assertEquals(
        "${HERON_LIB}/statemgr/*",
        ConfigDefaults.get(Key.STATEMGR_CLASSPATH)
    );
    Assert.assertEquals(
        "${HERON_LIB}/uploader/*",
        ConfigDefaults.get(Key.UPLOADER_CLASSPATH)
    );
  }

  @Test
  public void testResources() throws Exception {
    Assert.assertEquals(ByteAmount.fromGigabytes(1), ConfigDefaults.getByteAmount(Key.STMGR_RAM));
    Assert.assertEquals(1.0, ConfigDefaults.getDouble(Key.INSTANCE_CPU), 0.001);
    Assert.assertEquals(ByteAmount.fromGigabytes(1),
        ConfigDefaults.getByteAmount(Key.INSTANCE_RAM));
    Assert.assertEquals(ByteAmount.fromGigabytes(1),
        ConfigDefaults.getByteAmount(Key.INSTANCE_DISK));
  }
}
